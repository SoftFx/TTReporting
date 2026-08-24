# TT Reporting — Production Deployment

## Structure

```
prod/
├── docker-compose.yml   # All services: gateway + web apps + batch jobs
├── caddy/
│   ├── Caddyfile        # Reverse proxy config (committed)
│   ├── users.caddyfile  # User/password hashes (gitignored, create manually)
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

### 2. Create users file

```bash
# Generate hash:
echo 'PASSWORD' | docker run --rm -i caddy:2-alpine caddy hash-password
```

Create `caddy/users.caddyfile` with content:

```
(admin_creads) {
    admin $2a$10$GENERATED_HASH_HERE
}

(all-users) {
    admin $2a$10$GENERATED_HASH_HERE
}
```

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

Available at `https://<server-ip>/`

### 5. Install certificate on client machines

Open the page → click **Install Root CA** in the header → save `root-ca.crt` → double-click → Install Certificate → **Local Machine** → **Trusted Root Certification Authorities** → Finish.

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
| `PG_PASSWORD` | all 5 batch jobs (PostgreSQL) |
| `HSM_PRODUCT_KEY` | all 5 batch jobs — soft-fx Health Status Monitor product key (`productKey` in `config.yaml`) |

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
| **Caddy files only** (`Caddyfile`, `users.caddyfile`, `products.yaml`) | ❌ no | ✅ yes | `reload-caddy.bat` |

> `config.yaml`, `.env`, and `docker-compose.yml` are **not pushed to the server by `git pull`** — the first two are gitignored, and compose is applied by you, not git. Whenever any of them changes, copy the updated file to the server manually.

### Deploy helper scripts (`prod/*.bat`)

All of them SSH into the server using `SERVER` / `CADDY_CONTAINER` / `REMOTE_PROD_DIR` from `prod/.env`.

| Script | Invocation | What it does |
|---|---|---|
| `deploy_product.bat` | double-click → type service name | `docker compose pull` + recreate container + validate/reload Caddy. **Full deploy** — use when the image or env changed. |
| `restart_product.bat` | double-click → type service name | `docker compose restart` only — re-reads the mounted `configDocker/`, **no** pull, **no** recreate, **no** Caddy reload. Use when **only** `config.yaml` changed and is already on the server. |
| `deploy-product.bat` | CLI: `deploy-product.bat <service>` | Same as `deploy_product.bat` but takes the service name as an argument (no double-click prompt). |
| `reload-caddy.bat` | double-click | Validate + reload Caddy only. Use when **only** Caddy files changed and the container is already running. |

> ⚠️ **For one-shot batch jobs (`restart: "no"`), `docker compose up -d <service>` doesn't just recreate the container — it actually RUNS the job to completion** (executes its default `CMD`), same as a real scheduled/cron trigger would. So deploying a batch job (e.g. `deploy_product.bat` → `tt-statements-monthly`) causes one real production execution as a side effect of the deploy itself, writing real output — not a no-op recreate. Confirmed 2026-08-19: deploying `tt-statements-monthly` generated real `.monthly` statement files at deploy time, before the Cronicle schedule ever fired. Don't mistake this output for a Cronicle-triggered run when checking timestamps/logs.

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

### 2. Create a user group in `caddy/users.caddyfile`

Define who has access to this product. Add a new snippet group with the product slug + `-users` suffix:

```
(my-product-users) {
    john.doe $2a$10$BCRYPT_HASH_HERE
    jane.doe $2a$10$BCRYPT_HASH_HERE
}
```

To generate a bcrypt hash for a new user, run on the server:

```bash
echo 'PASSWORD' | docker exec -i caddy caddy hash-password
```

Or use `add-user.bat` from a Windows machine:

```bat
prod\add-user.bat john.doe
```

### 3. Add the new users to `all-users` snippet

Open `caddy/users.caddyfile` and add every user from the new group to the `(all-users)` snippet. This gives them access to the home page so they can see the product cards:

```
(all-users) {
    ...existing users...
    john.doe $2a$10$BCRYPT_HASH_HERE
    jane.doe $2a$10$BCRYPT_HASH_HERE
}
```

### 4. Add route and auth in `caddy/Caddyfile`

Inside the `:443 { ... }` block, add a `handle` block. Auth and proxy must be in the **same** handle block:

```
# ── My Product ───────────────────────────────
handle /my-product/* {
    basic_auth {
        import admin_creads
        import my-product-users
    }
    uri strip_prefix /my-product
    reverse_proxy my-product-container:8080
}
```

How it works:
- `handle /my-product/*` — matches all requests to `/my-product/...`
- `basic_auth` — only `admin_creads` (full admins) and `my-product-users` can access
- `uri strip_prefix /my-product` — removes the prefix before forwarding
- `reverse_proxy my-product-container:8080` — forwards to the container

### 5. Add service in `docker-compose.yml`

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

### 6. Deploy

On the server:

```bash
cd /path/to/prod

# Pull the new image
docker compose pull my-product

# Start the new service and reload Caddy
docker compose up -d my-product
docker exec caddy caddy reload --config /etc/caddy/Caddyfile
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
| `caddy/users.caddyfile` | Add `<slug>-users` group + update `all-users` |
| `caddy/Caddyfile` | Add `handle` block with auth + proxy |
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
- **`caddy/Caddyfile`** — new `handle /statements/*` route: `basic_auth` (`admin_creads` +
  `statements-users`), `reverse_proxy seaweedfs:8888`, restricted to **GET/HEAD only** (a
  `@notget` matcher responds `405` to anything else) — writes must stay unreachable from
  outside; only the batch job, over the internal Docker network, can PUT. A second,
  **unauthenticated** route `handle /seaweedfsstatic/*` proxies the filer's own CSS/logo
  assets (its folder-browser UI loads them from an absolute path outside `/statements/*`) —
  cosmetic only, doesn't affect the statement files themselves.
- **`caddy/users.caddyfile.example`** — new `(statements-users)` group, same placeholder
  pattern as the other per-product groups. On the **real** `caddy/users.caddyfile` (gitignored,
  server-only), keep this group **empty** if all current users are already admins — Caddy
  rejects `basic_auth` with a username duplicated across imported snippets
  (`username is not unique`), so don't also list an `admin_creads` user here.

Full deployment story (server steps, verification, bugs hit) is in
`source/tt-statements/seaweed-test-docker/NOTES.md`.

## Adding a new user

### Option A: from Windows (add-user.bat)

```bat
prod\add-user.bat john.doe
```

This generates a random 12-character password and prints the bcrypt hash. Copy the output line into `caddy/users.caddyfile`.

### Option B: from the server

```bash
# SSH to server
cd /path/to/prod

# 1. Generate bcrypt hash
echo 'PASSWORD' | docker exec -i caddy caddy hash-password

# 2. Add to caddy/users.caddyfile — paste into the right group(s)
nano caddy/users.caddyfile

# 3. Also add to (all-users) so they can see the home page

# 4. Reload Caddy
docker exec caddy caddy reload --config /etc/caddy/Caddyfile
```
