#!/bin/bash
# Generate Root CA + server certificate signed by that CA (valid 10 years)
# Run once on the production server
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -z "$1" ]; then
    echo "Usage: bash generate-cert.sh <IP1,IP2,...>"
    echo "Example: bash generate-cert.sh 195.13.245.138,192.168.10.32"
    exit 1
fi

if [ -f "$SCRIPT_DIR/server.crt" ] && [ -f "$SCRIPT_DIR/server.key" ]; then
    echo "Certificate already exists. To regenerate, delete server.crt and server.key first."
    exit 0
fi

# Build SAN from comma-separated IPs
SAN=$(echo "$1" | tr ',' '\n' | sed 's/^/IP:/' | tr '\n' ',' | sed 's/,$//')
echo "Certificate SAN: $SAN"

# 1. Generate Root CA
openssl req -x509 -newkey rsa:2048 \
    -keyout "$SCRIPT_DIR/root-ca.key" \
    -out "$SCRIPT_DIR/root-ca.crt" \
    -days 3650 -nodes \
    -subj '/CN=TT Reporting Root CA' \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign'

# 2. Generate server key + CSR
openssl req -newkey rsa:2048 \
    -keyout "$SCRIPT_DIR/server.key" \
    -out "$SCRIPT_DIR/server.csr" \
    -nodes \
    -subj '/CN=TT Reporting'

# 3. Sign server cert with Root CA
openssl x509 -req \
    -in "$SCRIPT_DIR/server.csr" \
    -CA "$SCRIPT_DIR/root-ca.crt" \
    -CAkey "$SCRIPT_DIR/root-ca.key" \
    -CAcreateserial \
    -out "$SCRIPT_DIR/server.crt" \
    -days 3650 \
    -extfile <(echo "basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=$SAN") \
    -sha256

# 4. Cleanup CSR
rm "$SCRIPT_DIR/server.csr"

echo ""
echo "Done!"
echo "  server.crt + server.key  -> for Caddy"
echo "  root-ca.crt              -> install on client machines (Trusted Root CA)"
echo "  root-ca.key              -> keep safe or delete after deployment"
