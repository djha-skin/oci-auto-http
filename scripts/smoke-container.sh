#!/usr/bin/env bash
set -euo pipefail

for command in docker openssl curl python3; do
  command -v "$command" >/dev/null || {
    echo "missing prerequisite: $command" >&2
    exit 2
  }
done

image="${OCI_AUTO_HTTP_SMOKE_IMAGE:-oci-auto-http:smoke}"
container="oci-auto-http-smoke-$$"
tmp="$(mktemp -d)"
port="$(python3 - <<'PY'
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
)"

cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
  rm -rf "$tmp"
}
trap cleanup EXIT

# This key and certificate are disposable test material, never deployment credentials.
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$tmp/server-key.pem" \
  -out "$tmp/server-chain.pem" \
  -days 1 -subj /CN=localhost \
  -addext 'subjectAltName=IP:127.0.0.1' >/dev/null 2>&1
chmod 0644 "$tmp/server-key.pem" "$tmp/server-chain.pem"
cat > "$tmp/config.toml" <<'EOF'
version = 1

[server]
listen = "0.0.0.0:8443"

[[mounts]]
name = "files"
prefix = "/files"
registry = "https://registry.example.com"
repository = "team/files"
EOF
chmod 0644 "$tmp/config.toml"

docker build --tag "$image" .
docker run --detach --name "$container" \
  --user 10001:10001 \
  --read-only \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --memory 384m \
  --cpus 1 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=256m,mode=1777 \
  --publish "127.0.0.1:${port}:8443" \
  --volume "$tmp/config.toml:/etc/oci-auto-http/config.toml:ro,z" \
  --volume "$tmp/server-chain.pem:/run/secrets/server-chain.pem:ro,z" \
  --volume "$tmp/server-key.pem:/run/secrets/server-key.pem:ro,z" \
  "$image" serve \
    --config /etc/oci-auto-http/config.toml \
    --tls-cert /run/secrets/server-chain.pem \
    --tls-key /run/secrets/server-key.pem >/dev/null

for attempt in $(seq 1 50); do
  status="$(curl --silent --show-error --max-time 2 \
    --cacert "$tmp/server-chain.pem" \
    --output /dev/null --write-out '%{http_code}' \
    "https://127.0.0.1:${port}/healthz" 2>/dev/null || true)"
  if [[ "$status" == "204" ]]; then
    echo "container-smoke: HTTPS /healthz returned 204 under non-root/read-only/384MiB profile"
    docker stop --time 35 "$container" >/dev/null
    exit 0
  fi
  sleep 0.1
done

docker logs "$container" >&2
echo "container did not become healthy on loopback port $port" >&2
exit 1
