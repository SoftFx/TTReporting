# TT Reporting — Production Deployment

## Structure

```
prod/
├── docker-compose.yml   # All services: gateway + web apps + batch jobs
├── authentik/
│   └── setup-authentik.py  # Creates/checks the Authentik (SSO) side, see "Authentication"
├── authentik-outpost/
│   └── outpost.env      # Outpost token (server only, gitignored)
├── caddy/
│   ├── Caddyfile        # Reverse proxy config (committed)
│   ├── Dockerfile       # Caddy + Cloudflare DNS module, built on the server
│   ├── caddy.env        # Host names, CF_API_TOKEN, PORTAL_IP (server only, gitignored)
│   ├── data/ config/    # Let's Encrypt certificates + ACME account (server only, keep)
│   ├── generate-cert.sh # Run once to generate TLS certificate
│   ├── server.crt       # Server certificate (generated, gitignored)
│   ├── server.key       # Server private key (generated, gitignored)
│   ├── root-ca.crt      # Root CA certificate (generated, gitignored)
│   ├── root-ca.key      # Root CA private key (generated, gitignored)
│   ├── static/          # Vendored JS libraries
│   ├── index.html       # Home page (reads products.yaml dynamically)
│   └── products.yaml    # Product registry (names, descriptions, categories)
└── aggr-data-app/
    └── configDocker/    # App config (gitignored)
```

## First-time setup

### 1. Generate TLS certificate

```bash
cd prod/caddy
bash generate-cert.sh <SERVER_IP>
# Example: bash generate-cert.sh 10.0.0.5
# For multiple IPs: bash generate-cert.sh 10.0.0.5,192.168.1.100
```

Run once. Generates Root CA + server certificate (valid 10 years) with the given IP(s) in SAN.

### 2. Set up SSO in Authentik

Access goes through Authentik (`<authentik-host>`), see [Authentication](#authentication-authentik-sso).
Run `prod/authentik/setup-authentik.py` once (needs `AUTHENTIK_TOKEN` in `prod/.env`), put the outpost
token into `authentik-outpost/outpost.env` on the server and start `docker compose up -d authentik-outpost`.

### 3. Create app config directories

```bash
mkdir -p aggr-data-app/configDocker
# Copy config files (dbCon_config.yaml, orderTypes.csv, commands.csv)
```

### 4. Start services

```bash
cd prod
docker compose up -d caddy aggr-data-app
```

Available at `https://<portal-host>/` (the IP address redirects there).

### 5. Certificates

The portal and Cronicle use **Let's Encrypt certificates for their DNS names**, obtained by Caddy via DNS-01
(see [TLS and DNS names](#tls-and-dns-names)); browsers need nothing installed. The self-signed Root CA from
step 1 is still used for the old IP address (it only redirects) and for S3 on `:8333`; S3 clients that connect
by IP need `root-ca.crt` (served at `https://<portal-ip>/root-ca.crt`).

## Running batch jobs

```bash
docker compose run --rm big-trades
docker compose run --rm diff-prices
# etc.
```

### Required environment variables

Batch jobs read secrets from the environment. On the server, create a `.env` file in `/opt/automation/jobs/` (next to `docker-compose.yml`) holding the real secrets — `docker compose` reads it automatically and injects the variables into each container via the `environment:` block defined in `docker-compose.yml`:

```dotenv
MT4_PASSWORD=...
MT5_PASSWORD=...
PG_PASSWORD=...
HSM_PRODUCT_KEY=81851f0c-...
```

> **Two different `.env` files — don't confuse them.** This repo contains **`prod/.env`**, but it holds **only SSH parameters for the deploy `.bat` scripts** (`SERVER`, `CADDY_CONTAINER`, `REMOTE_PROD_DIR`) — **no passwords**. The secrets above live in a **separate** `.env` on the server at `/opt/automation/jobs/.env`, which is not part of this repo.

| Variable | Used by |
|---|---|
| `MT4_PASSWORD`, `MT5_PASSWORD` | `big-trades`, `count-trades`, `big-deposits`, `negative-equity` (MariaDB) |
| `PG_PASSWORD` | all 6 batch jobs (PostgreSQL) |
| `HSM_PRODUCT_KEY` | all 6 batch jobs — soft-fx Health Status Monitor product key (`productKey` in `config.yaml`) |

Each job's `configDocker/config.yaml` references these as `${VAR}` placeholders. The config file itself is gitignored (real hosts/users), so when it changes you must **copy the updated `config.yaml` to the server manually** — it does not arrive via `git pull`. See the root [`README.md`](../README.md#secrets--environment-variables-batch-r-projects) for the full secrets/env pattern.

> **Apply config/env changes with a recreate, not a restart.** `docker compose restart` does **not** re-read the `environment:` block or `.env` for container variables — the new `HSM_PRODUCT_KEY` only takes effect when the container is recreated (`docker compose up -d <job>` or `deploy_product.bat`).

## Updating an existing project

What you do depends on **what changed**. The decision tree below covers the three common cases. Server files live under `/opt/automation/jobs/` (= `REMOTE_PROD_DIR` in `prod/.env`).

### What changed → which steps

| What changed | Rebuild image? | Copy files to server? | Deploy command |
|---|---|---|---|
| **R/Python code** (`source/**`, `sourcePython/**`, Dockerfile) | ✅ yes | only if `config.yaml` also changed | `deploy_product.bat` |
| **`config.yaml` only** (`configDocker/config.yaml`) | ❌ no | ✅ yes — `config.yaml` is gitignored, must be copied manually | `restart_product.bat` |
| **`.env` / `docker-compose.yml`** (env vars, e.g. `HSM_PRODUCT_KEY`) | ❌ no | ✅ yes — update server `.env` / `docker-compose.yml` manually | `deploy_product.bat` (needs recreate, see note below) |
| **Caddy files only** (`Caddyfile`, `products.yaml`, `index.html`) | ❌ no | ✅ yes | `reload-caddy.bat` |
| **Caddy image** (`caddy/Dockerfile`) | ✅ yes, on the server: `docker compose build caddy` | ✅ yes | `docker compose up -d caddy` |
| **Users / access** | ❌ no | ❌ no | Authentik, see [Adding a user](#adding-a-user) |

> `config.yaml`, `.env`, and `docker-compose.yml` are **not pushed to the server by `git pull`** — the first two are gitignored, and compose is applied by you, not git. Whenever any of them changes, copy the updated file to the server manually.

### Deploy helper scripts (`prod/*.bat`)

All of them SSH into the server using `SERVER` / `CADDY_CONTAINER` / `REMOTE_PROD_DIR` from `prod/.env`.

| Script | Invocation | What it does |
|---|---|---|
| `deploy_product.bat` | double-click → type service name | `docker compose pull` + recreate container + validate/reload Caddy. **Full deploy** — use when the image or env changed. **Not for `caddy`** — its image is built on the server, not pulled. |
| `restart_product.bat` | double-click → type service name | `docker compose restart` only — re-reads the mounted `configDocker/`, **no** pull, **no** recreate, **no** Caddy reload. Use when **only** `config.yaml` changed and is already on the server. |
| `deploy-product.bat` | CLI: `deploy-product.bat <service>` | Same as `deploy_product.bat` but takes the service name as an argument (no double-click prompt). |
| `reload-caddy.bat` | double-click | Validate + reload Caddy only. Use when **only** Caddy files changed and the container is already running. |

> ⚠️ **For one-shot batch jobs (`restart: "no"`), `docker compose up -d <service>` doesn't just recreate the container — it actually RUNS the job to completion** (executes its default `CMD`), same as a real scheduled/cron trigger would. So deploying a batch job (e.g. `deploy_product.bat` → `tt-statements-monthly`) causes one real production execution as a side effect of the deploy itself, writing real output — not a no-op recreate. Confirmed 2026-08-19: deploying `tt-statements-monthly` generated real `.monthly` statement files at deploy time, before the Cronicle schedule ever fired. Don't mistake this output for a Cronicle-triggered run when checking timestamps/logs. `restart_product.bat` has the same effect (`docker compose restart` starts an exited one-shot container again, re-running it) — there's no way to apply a code/config change to one of these jobs without triggering one real execution.
>
> **Trick to make that forced execution cheap while testing code changes:** if the job's `config.yaml` supports scoping to a subset (e.g. `tt-statements`' domain list), temporarily restrict it to the smallest/cheapest subset before deploying — the forced run completes in seconds instead of tens of minutes. Restore the full config afterward; you don't need to force another manual run immediately, since a one-shot container re-reads its mounted config fresh on every invocation — the *next* Cronicle-scheduled trigger will pick up the restored full config on its own. Only run `restart_product.bat` again if you want to verify the full run right now instead of waiting for the schedule. Used for `tt-statements` 2026-08-24/25, see `source/tt-statements/seaweed-test/NOTES.md`.

### Full procedure: code changed

1. **Commit & push** to `main`.
2. **Build the image** on GitHub: trigger `release-dockerhub` (workflow_dispatch, variant = `<project>`) and **wait for the green build** → new `softfx/tt-reporting:latest-<project>` on Docker Hub.
3. **Copy changed files to the server** if any (gitignored `config.yaml`, or updated `.env` / `docker-compose.yml`).
4. **Deploy**: double-click `deploy_product.bat` → type the service name. It pulls the new image, recreates the container, and reloads Caddy.

### Full procedure: config only changed

1. **Copy** the updated `configDocker/config.yaml` to the server (`/opt/automation/jobs/<project>/configDocker/config.yaml`).
2. **Restart**: double-click `restart_product.bat` → type the service name. (No build, no pull — the running image just re-reads its mounted config.)

> ⚠️ **`restart_product.bat` is NOT enough for `.env` / `docker-compose.yml` changes.** `docker compose restart` does not re-read the `environment:` block or `.env` for container variables. A new env var (like `HSM_PRODUCT_KEY`) only takes effect when the container is **recreated** — use `deploy_product.bat` for those.

## Adding a new product

### 1. Add product entry to `caddy/products.yaml`

This file drives the home page cards. Each product needs a unique `slug` (used in URLs).

```yaml
- slug: my-product
  name: My Product
  description: |
    Short description shown on the home page card.
  category: Analytics
  # image: static/image/my-product.png   # optional
```

### 2. Access

Nothing to do in Authentik: `tt-reporting-admins` and `tt-reporting-users` see every product.
(For an admin-only product use a `@deny_admin` matcher instead of `@deny_portal`, see the comment in `caddy/Caddyfile`.)

### 3. Add route in `caddy/Caddyfile`

Inside the `(portal) { ... }` snippet (it is imported by the `{$PORTAL_HOST}` site), add a `handle` block. The SSO gate, the group check and the proxy must be in one `route` (a `route` keeps the order as written):

```
# ── My Product ───────────────────────────────
handle /my-product/* {
    route {
        import sso
        respond @deny_portal 403
        uri strip_prefix /my-product
        reverse_proxy my-product-container:8080
    }
}
```

How it works:
- `import sso` — asks Authentik; not logged in → redirect to the SSO login; logged in → `X-Authentik-*` headers are set on the request
- `respond @deny_portal 403` — only members of `tt-reporting-admins` / `tt-reporting-users` pass
- `uri strip_prefix /my-product` — removes the prefix before forwarding
- `reverse_proxy my-product-container:8080` — forwards to the container (it can read the user from `X-Authentik-Username`)

### 4. Add service in `docker-compose.yml`

Add the container definition. It must be on the `tt-internal` network (Caddy routes traffic through it) and must NOT expose ports to the host (Caddy handles that):

```yaml
my-product:
  image: softfx/tt-reporting:latest-my-product
  pull_policy: always
  volumes:
    - ./my-product/configDocker:/app/configDocker:ro
  restart: unless-stopped
  networks:
    - tt-internal
```

### 5. Deploy

On the server:

```bash
cd /path/to/prod

# Pull the new image
docker compose pull my-product

# Start the new service and reload Caddy
docker compose up -d my-product
docker compose exec caddy caddy reload --config /etc/caddy/Caddyfile
```

Or from Windows:

```bat
prod\deploy-product.bat my-product
```

This pulls the service image, starts the service, validates the Caddy config, and reloads Caddy.
Use `prod\reload-caddy.bat` when only Caddy files changed and the product container is already running.

### Summary: files to edit

| File | What to change |
|---|---|
| `caddy/products.yaml` | Add product card entry |
| `caddy/Caddyfile` | Add `handle` block with `import sso` + `@deny_portal` + proxy |
| `docker-compose.yml` | Add service container |

## Storage backend: SeaweedFS (tt-statements)

`tt-statements` writes its generated HTML statements to SeaweedFS instead of local disk.
SeaweedFS is treated as **infrastructure, like Caddy** — not a product: it's the official
`chrislusf/seaweedfs` image, no custom code, no `products.yaml` card.

Changes made to the shared prod files for this:

- **`docker-compose.yml`** — new `seaweedfs` service (`server -dir=/data -filer -ip=0.0.0.0`,
  data volume `./seaweedfs/data:/data`, healthcheck on port 8888). It sits on **both**
  `tt-internal` and `default` networks, because the always-on services live on `tt-internal`
  while the batch jobs (no explicit `networks:` key) live on the implicit `default` network —
  it needs to be reachable from both. `tt-statements-daily`/`tt-statements-monthly` gained
  `SEAWEED_FILER_URL: http://seaweedfs:8888` (Docker-internal DNS name — **not a secret**,
  hardcoded directly here, not routed through `.env`) and `depends_on: seaweedfs: condition:
  service_healthy`.
- **`caddy/Caddyfile`** — two routes, both behind the SSO gate (`import sso` + `@deny_portal`):
  `handle /statements/*` (the home-page card) only redirects to `/buckets/...`;
  `handle @statement_buckets` (`/buckets/`, `/buckets/statements/*`, `/buckets/statements-prom/*`)
  does `reverse_proxy seaweedfs:8888`, restricted to **GET/HEAD only** (a `@notget` matcher
  responds `405` to anything else) — writes must stay unreachable from outside; only the batch
  job, over the internal Docker network, can PUT. A third, **unauthenticated** route
  `handle /seaweedfsstatic/*` proxies the filer's own CSS/logo assets (its folder-browser UI loads
  them from an absolute path outside `/buckets/`) — cosmetic only, doesn't affect the statement
  files themselves.

Full deployment story (server steps, verification, bugs hit) is in
`source/tt-statements/SEAWEED_DEPLOYMENT.md` (written before the SSO switch, so it still
describes `basic_auth`).

**Two buckets since 2026-10-12.** `ttlivePROM` is a separate product with its own S3 key, so it
lives in its own bucket: `/buckets/statements` holds `ttlive` / `ttliveUK` / `ttliveEU`,
`/buckets/statements-prom` holds `ttlivePROM`. What that changed in the shared prod files:

- **`caddy/Caddyfile`** — `/statements/` (the home-page card) now redirects to `/buckets/`, the
  level where both buckets are visible; deeper `/statements/<domain>/...` bookmarks still land in
  the first bucket. One named matcher serves exactly `/buckets/`, `/buckets/statements/*` and
  `/buckets/statements-prom/*` (GET/HEAD only, same auth) — a new bucket is not reachable until
  it is added there on purpose.
- **`seaweedfs/s3.json`** (gitignored, see `s3.json.example`) — one identity per key, rights are
  per **bucket** (`Read:<bucket>` + `List:<bucket>`). Per-folder rights inside a bucket break
  ordinary S3 clients (HeadBucket / GetBucketLocation need bucket-wide `Read`).
- **`tt-statements/configDocker/config.yaml`** (gitignored) — `storage.seaweed_buckets` maps every
  domain to its bucket; there is no default, a domain missing from the map fails that domain.
- **Updating `caddy/Caddyfile` on the server:** overwrite it in place (`sudo cp new old`), do not
  replace it with `mv`. It is bind-mounted as a single file and Caddy is only reloaded, so a
  replaced file (new inode) stays invisible to the container until it is recreated.

Details and the rollout steps: `source/tt-statements/SEAWEED_PROM_BUCKET.md`.

## Authentication (Authentik SSO)

Since 2026-11 the portal uses **Authentik SSO** (`https://<authentik-host>`) instead of Caddy `basic_auth`
(the old `users.caddyfile` is no longer imported).

### Caddy: what to do

| Task | How |
|---|---|
| Changed `Caddyfile` / `index.html` / `products.yaml` | copy to server (`sudo cp`, not `mv`) → `reload-caddy.bat` |
| Changed `caddy/Dockerfile` | on server: `docker compose build caddy && docker compose up -d caddy` |
| Give someone access | Authentik group `tt-reporting-users`, see [Adding a user](#adding-a-user) |
| Every page answers 502 | check the `authentik-outpost` container |
| S3 downloads (`:8333`) | not behind SSO; keys in `seaweedfs/s3.json` |

### Server files & secrets

| File (server, `/opt/automation/jobs/`) | Variables / content | Secret? |
|---|---|---|
| `caddy/caddy.env` | `PORTAL_HOST`, `CRONICLE_HOST`, `PORTAL_ACME_ALIAS`, `CRONICLE_ACME_ALIAS` | no |
| | `PORTAL_IP` | no (but never commit) |
| | `CF_API_TOKEN` — Cloudflare token, DNS edit of the stub zone only | **yes** |
| `authentik-outpost/outpost.env` | `AUTHENTIK_HOST`, `AUTHENTIK_HOST_BROWSER` | no |
| | `AUTHENTIK_TOKEN` — outpost token | **yes** |
| `caddy/data/` | Let's Encrypt certificates + ACME account key | sensitive, **keep** |
| `/opt/automation/cronicle/.env` | `CRONICLE_HOST` | no |
| `prod/.env` (your PC, not server) | `AUTHENTIK_URL`, `TT_REPORTING_URL`; `AUTHENTIK_TOKEN` — Authentik admin API token for `setup-authentik.py` | **yes** (token) |

Unchanged: `.env` (DB passwords, `HSM_PRODUCT_KEY`), `seaweedfs/s3.json` (S3 keys), `caddy/server.crt`/`server.key`.
No longer used: `caddy/users.caddyfile`.

### How it works

- **Authentik side:** application `TT Reporting` (slug `tt-reporting`), Proxy provider in *Forward auth
  (single application)* mode, external host `https://<portal-host>`, served by the outpost
  **`tt-reporting outpost`**. Created by `prod/authentik/setup-authentik.py` (idempotent, `--dry-run`
  shows what it would do). The embedded outpost on `<authentik-host>` is not used: in this installation
  it answers 404 for other hosts (the logs server has its own outpost for the same reason).
- **Outpost container:** `authentik-outpost` in `docker-compose.yml` (`ghcr.io/goauthentik/proxy`, tag =
  Authentik server version), only on `tt-internal`, no published ports. It keeps one connection to
  `<authentik-host>`. Its token is in `/opt/automation/jobs/authentik-outpost/outpost.env`
  (`AUTHENTIK_TOKEN=...`, mode 640, gitignored). When Authentik is upgraded, bump the image tag.
- **Caddy side:** the `(sso)` snippet calls `authentik-outpost:9000/outpost.goauthentik.io/auth/caddy`
  (`forward_auth`); `/outpost.goauthentik.io/*` (login callback, sign out) is proxied to the outpost.
  Client-sent `X-Authentik-*` headers are dropped before the check. If the outpost is down, every
  page answers 502.
- **Access:** two Authentik groups, both bound to the application (nobody else can log in):
  `tt-reporting-admins` and `tt-reporting-users`. Both currently see every product; Caddy re-checks the
  `X-Authentik-Groups` header (`@deny_portal`). Group names and members are changed only with the owner's approval.
- **Server-only values (gitignored, never commit real hosts or IPs):** `authentik-outpost/outpost.env`
  (`AUTHENTIK_HOST`, `AUTHENTIK_HOST_BROWSER` = `https://<authentik-host>`, `AUTHENTIK_TOKEN`) and
  `caddy/caddy.env` (host names, ACME aliases, `CF_API_TOKEN`, `PORTAL_IP`; see the header of `caddy/Caddyfile`).
- **Not affected:** port `8333` (S3, own keys, self-signed certificate by IP) and `/root-ca.crt` (public).
- **Sign out:** `/outpost.goauthentik.io/sign_out` (link on the home page).

### Cronicle behind SSO

Cronicle (`/opt/automation/cronicle`, `network_mode: host`) is no longer open on `http://<ip>:3012`.

- **URL:** `https://<cronicle-host>` — Caddy site, SSO gate, **`tt-reporting-admins` only** (Authentik
  application `TT Reporting Cronicle`, same outpost). The old `https://<portal-ip>:3443` redirects there.
- **One login:** Cronicle's `User.external_user_api` = `https://<cronicle-host>/sso/cronicle-user`. The Cronicle
  server sends the browser's cookies there, Caddy checks them with the outpost and returns the user as a Cronicle
  admin; Cronicle creates the account on first login. The server is behind NAT and cannot reach its own public
  IP, so the Cronicle container maps `<cronicle-host>` to `172.17.0.1` (`extra_hosts`, name from
  `/opt/automation/cronicle/.env`, `CRONICLE_HOST=...`). Logout (`?logout=1`) signs out of SSO.
- **Cronicle `conf/config.json` (server only):** `WebServer.http_bind_address` = `172.17.0.1` (Caddy reaches it as
  `host.docker.internal:3012`), `User.external_user_api` as above, `base_app_url` = `https://<cronicle-host>`.
- **Restart note:** after a Cronicle restart it waits ~60 s before becoming master; runs due in that minute are
  skipped unless the event has Catch-Up enabled.

### TLS and DNS names

- `<portal-host>` and `<cronicle-host>` point at the public IP. Certificates: Let's Encrypt via **DNS-01**;
  `_acme-challenge.<name>` is a CNAME into a stub zone on Cloudflare, Caddy writes the TXT record there
  (`dns_challenge_override_domain`) with a token limited to that zone. No inbound port 80 is needed.
- Caddy image: `caddy/Dockerfile` (Caddy 2.11.4 + `caddy-dns/cloudflare` v0.2.4), built on the server:
  `docker compose build caddy`. Certificates and the ACME account live in `caddy/data` (keep it: without it every
  recreate re-issues and runs into Let's Encrypt rate limits).
- The IP site has an explicit address (`https://{$PORTAL_IP}`) and `default_sni` is the IP: a catch-all `:443`
  site with a certificate file would become the TLS policy for every name and hide the Let's Encrypt certificates.

### Adding a user

Users are not created on the server any more. The user needs an Authentik account; then add them to the
`tt-reporting-users` (or `tt-reporting-admins`) — in the Authentik admin UI, or, after the owner approves it:

```bash
python prod/authentik/setup-authentik.py --group tt-reporting-users --member tt-reporting-users=john.doe
```

The script never creates groups or adds people unless they are passed explicitly.

### Obsolete since the SSO switch (2026-11)

| File | Why |
|---|---|
| `caddy/users.caddyfile` (server) | no longer imported by the Caddyfile; contained bcrypt hashes — archive or remove |
| `caddy/users.caddyfile.example` | template for the old basic_auth groups |
| `add-user.bat` | generated bcrypt hashes for `users.caddyfile`; users are now in Authentik |
| `.gitignore` entry `/prod/caddy/users.caddyfile` | file no longer used |
