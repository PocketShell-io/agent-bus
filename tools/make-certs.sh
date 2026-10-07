#!/bin/bash
set -euo pipefail
OUTDIR=${1:-.local/certs}
mkdir -p "$OUTDIR"
chmod 0700 "$OUTDIR"

# CA
openssl req -x509 -newkey rsa:2048 -days 365 -nodes -keyout "$OUTDIR/ca.key" -out "$OUTDIR/ca.crt" -subj "/CN=TestCA" >/dev/null 2>&1

# Server
openssl req -newkey rsa:2048 -nodes -keyout "$OUTDIR/server.key" -out "$OUTDIR/server.csr" -subj "/CN=localhost" >/dev/null 2>&1
openssl x509 -req -in "$OUTDIR/server.csr" -CA "$OUTDIR/ca.crt" -CAkey "$OUTDIR/ca.key" -CAcreateserial -out "$OUTDIR/server.crt" -days 365 >/dev/null 2>&1

# Client 1
openssl req -newkey rsa:2048 -nodes -keyout "$OUTDIR/client.key" -out "$OUTDIR/client.csr" -subj "/CN=TestClient" >/dev/null 2>&1
openssl x509 -req -in "$OUTDIR/client.csr" -CA "$OUTDIR/ca.crt" -CAkey "$OUTDIR/ca.key" -CAcreateserial -out "$OUTDIR/client.crt" -days 365 >/dev/null 2>&1

# Client 2 (unauthorized)
openssl req -newkey rsa:2048 -nodes -keyout "$OUTDIR/client2.key" -out "$OUTDIR/client2.csr" -subj "/CN=BadClient" >/dev/null 2>&1
openssl x509 -req -in "$OUTDIR/client2.csr" -CA "$OUTDIR/ca.crt" -CAkey "$OUTDIR/ca.key" -CAcreateserial -out "$OUTDIR/client2.crt" -days 365 >/dev/null 2>&1

echo "Certs generated in $OUTDIR"
