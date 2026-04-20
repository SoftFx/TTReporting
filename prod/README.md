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
│   ├── index.html       # Home page (reads products.yaml dynamically)
│   └── products.yaml    # Product registry (names, descriptions, categories)
└── aggr-data-app/
    └── configDocker/    # App config (gitignored)
```

## First-time setup

### 1. Generate TLS certificate

```bash
cd prod/caddy
bash generate-cert.sh
```

Run once. Generates Root CA + server certificate (valid 10 years). Server IP is auto-detected and included in SAN.

### 2. Create users file

```bash
# Generate hash:
docker run --rm caddy:2-alpine caddy hash-password --plaintext 'PASSWORD'
```

Create `caddy/users.caddyfile` with content:

```
(admin_creads) {
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

### 1. Add to `caddy/products.yaml`

```yaml
- slug: product-name
  name: Product Name
  description: |
    Description here.
  category: Category
```

### 2. Add route in `caddy/Caddyfile`

Inside the `:443 { ... }` block, add:

```
handle /product-name/ {
    basic_auth {
        import admin_creads
        # import product-name-users   (if defined in users.caddyfile)
    }
}
import service_proxy product-name product-name-container:PORT
```

### 3. Add service in `docker-compose.yml`

```yaml
product-name:
  image: softfx/tt-reporting:latest-product-name
  pull_policy: always
  volumes:
    - ./product-name/configDocker:/app/configDocker:ro
  restart: unless-stopped
  networks:
    - tt-internal
```

### 4. Restart

```bash
docker compose up -d
```

## Adding a new user

All commands run on the server via SSH from any machine:

```bash
ssh apazniak@195.13.245.138
cd /opt/automation/jobs

# 1. Generate bcrypt hash for the password
docker exec jobs-caddy-1 caddy hash-password --plaintext 'PASSWORD'
# Output: $2a$10$xxxxx...

# 2. Add user to caddy/users.caddyfile
nano caddy/users.caddyfile
# Paste the hash under the appropriate snippet, e.g.:
#   (admin_creads) {
#       admin $2a$10$existing...
#       newuser $2a$10$xxxxx...    <- add here
#   }

# 3. If the user needs access to a specific service, add to Caddyfile
nano caddy/Caddyfile
# Add username + hash in the service's basic_auth block

# 4. Restart Caddy to apply changes
docker compose restart caddy
```
