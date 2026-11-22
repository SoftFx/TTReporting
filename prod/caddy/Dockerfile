# Caddy with the Cloudflare DNS module: Let's Encrypt certificates via DNS-01
# through the delegated stub zone (CNAME _acme-challenge.<name> -> <alias>),
# same pinned versions as the HSM Caddy image.
# Built on the server:  docker compose build caddy
FROM caddy:2.11.4-builder-alpine AS builder
RUN xcaddy build v2.11.4 --with github.com/caddy-dns/cloudflare@v0.2.4

FROM caddy:2.11.4-alpine
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
