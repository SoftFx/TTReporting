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

Or from Windows: `prod\reload-caddy.bat`

### Summary: files to edit

| File | What to change |
|---|---|
| `caddy/products.yaml` | Add product card entry |
| `caddy/users.caddyfile` | Add `<slug>-users` group + update `all-users` |
| `caddy/Caddyfile` | Add `handle` block with auth + proxy |
| `docker-compose.yml` | Add service container |

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
