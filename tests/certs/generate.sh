#!/bin/bash
# Generate self-signed certificates for Redis TLS testing
set -e

cd "$(dirname "$0")"

# Generate CA key and certificate
openssl genrsa -out ca.key 4096
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 \
    -out ca.crt -subj "/CN=Test CA"

# Generate Redis server key and CSR
openssl genrsa -out redis.key 2048
openssl req -new -key redis.key -out redis.csr \
    -subj "/CN=localhost"

# Sign the Redis certificate with our CA
openssl x509 -req -in redis.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out redis.crt -days 365 -sha256

# Cleanup CSR
rm -f redis.csr

echo "Certificates generated successfully"
