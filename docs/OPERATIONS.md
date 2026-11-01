# Operator and client guide

This guide covers installation, deploy-time security, service probes, backup/recovery, and examples for static package repositories. See [README.md](../README.md) for the configuration and HTTP contracts. The checked-in config and Compose examples use placeholder registry names and contain no credentials.

## Supported Linux builds

The release workflow builds and smoke-tests these GNU/Linux targets:

| Rust target | Intended systems |
| --- | --- |
| `x86_64-unknown-linux-gnu` | 64-bit x86 Linux with glibc |
| `aarch64-unknown-linux-gnu` | 64-bit ARM Linux with glibc, including 64-bit Raspberry Pi OS |
| `armv7-unknown-linux-gnueabihf` | 32-bit ARM hard-float Linux (ARMv7) |

ARMv6/soft-float, musl, macOS, Windows, and other targets are not built or claimed by this workflow. Cross-target CI uses QEMU smoke tests; these are not measurements on physical ARM hardware. Build/release artifacts are versioned tarballs with SHA-256 sidecars, containing the binary, Rust source/build metadata, license, container/systemd assets, example configs, and operator/conformance docs.

For a source build on a supported host:

```sh
cargo build --release --locked
cargo test --locked --offline
sudo install -m 0755 target/release/oci-auto-http /usr/local/bin/oci-auto-http
```

For a release tarball, verify its sidecar before installing:

```sh
sha256sum --check oci-auto-http-<version>-<target>.tar.gz.sha256
tar -xzf oci-auto-http-<version>-<target>.tar.gz
sudo install -m 0755 oci-auto-http /usr/local/bin/oci-auto-http
```

The tarball is a dynamically linked GNU/Linux binary; use it on a compatible glibc system. The [Dockerfile](../Dockerfile) builds a Debian/glibc image for the architecture on which Docker builds it. Multi-architecture release binaries are produced by CI; the example does not publish container images.

## Configuration and credentials

Start from [`config.example.toml`](../config.example.toml) for a host install or [`deploy/config.container.example.toml`](../deploy/config.container.example.toml) for the container. Replace the example registry origin and repository names. `mount create` can add more mounts; `list`/`show` inspect them, and `update`/`delete` manage them. Mount edits are local TOML changes and do not update a running process: restart `serve` after edits.

Keep credentials out of TOML, command arguments, environment variables, Compose files, and logs. Use a dedicated registry repository and grant the service only repository-scoped pull/read access. The HTTP service never stores credentials. For caller authentication, enable direct inbound TLS with `--tls-cert`/`--tls-key`; the request's `Authorization` is forwarded only to the selected configured registry over HTTPS. TLS termination at a proxy is not supported for authenticated requests. The service does not trust `Forwarded` or `X-Forwarded-Proto`.

The built-in `artifact` CLI currently uses anonymous registry access. It can publish to anonymous/write-enabled repositories, but it is not a credential helper for protected registries. The public `RegistryClient` library accepts an optional Authorization header for programmatic operations; see the API docs in the README. Do not make a private write repository anonymous merely to enable CLI publishing—use a protected, separately operated publishing workflow instead.

Do not expose the example Compose binding publicly. Public deployment remains subject to the production-readiness caveats in the README and the remaining R6 work.

## Docker Compose

The example binds the host port to loopback, runs as a non-root UID, drops Linux capabilities, uses a read-only root filesystem, limits the container to one CPU/384 MiB, and gives private autoindex spools a bounded `/tmp`. It assumes direct HTTPS and an existing server certificate/key. It does not include a healthcheck command in the minimal image; use an external probe. To run the automated image/start/health smoke (`./scripts/smoke-container.sh`), install Docker, OpenSSL, curl, and Python 3; the build pulls the Rust and Debian base images if they are not already cached.

```sh
cp deploy/config.container.example.toml deploy/config.toml
# Edit deploy/config.toml and replace every registry/repository placeholder.
# Put the server certificate chain and private key at these paths.
mkdir -p deploy
chmod 0644 deploy/server-chain.pem
# The key must be readable by UID/GID 10001 inside the container, but not by other users.
sudo chown root:10001 deploy/server-key.pem
sudo chmod 0640 deploy/server-key.pem
docker compose up --build --detach
```

Probe it locally and inspect logs:

```sh
curl --fail --silent --show-error --cacert ./deploy/server-ca.pem \
  https://127.0.0.1:8443/healthz -o /dev/null
docker compose logs --follow oci-auto-http
```

`deploy/server-ca.pem` is the trust root for the listener certificate used by the probe; it is not mounted into the service. The upstream registry's private CA, if needed, is supplied separately with `--ca-cert` (add the read-only mount and option to `compose.yaml`). Compose bind-mounts are examples: create the local files first, protect the private key, and do not commit them. The `:z` suffix applies a shared SELinux container label on SELinux hosts; on other Docker hosts it is ignored. For authenticated requests, the service listener must remain directly TLS-terminated; do not place an HTTP-terminating reverse proxy in front of it.

## systemd

The sample unit [`deploy/oci-auto-http.service`](../deploy/oci-auto-http.service) runs as a dedicated unprivileged account, loads TLS materials as systemd credentials, has a read-only system view and private `/tmp`, and sets `TimeoutStopSec=45s`, longer than the default 30-second application drain deadline.

```sh
sudo useradd --system --user-group --no-create-home --home-dir /nonexistent \
  --shell /usr/sbin/nologin oci-auto-http
sudo install -d -o root -g oci-auto-http -m 0750 /etc/oci-auto-http
sudo install -d -o root -g root -m 0700 /etc/oci-auto-http/tls
sudo install -o root -g oci-auto-http -m 0640 config.example.toml \
  /etc/oci-auto-http/config.toml
# Edit config.toml; for this direct-TLS unit, set listen = "127.0.0.1:8443".
sudo install -o root -g root -m 0644 server-chain.pem \
  /etc/oci-auto-http/tls/server-chain.pem
sudo install -o root -g root -m 0600 server-key.pem \
  /etc/oci-auto-http/tls/server-key.pem
sudo install -m 0644 deploy/oci-auto-http.service \
  /etc/systemd/system/oci-auto-http.service
sudo systemctl daemon-reload
sudo systemctl enable --now oci-auto-http.service
sudo systemctl status oci-auto-http.service
sudo journalctl -u oci-auto-http.service
```

Adjust the config's listener and any systemd resource limits to the host profile. The sample `MemoryMax=384M`/`CPUQuota=100%` is the synthetic profile measured in the Linux tests, not a guarantee for every workload or physical Raspberry Pi. `SIGTERM` stops accepting requests and drains in-flight work for up to `graceful_shutdown_timeout_seconds`; keep the service manager's stop timeout longer than that setting.

## Health, readiness, and logs

`GET` or `HEAD /healthz` returns `204` when the HTTP process is responding. It is a **liveness** probe only: it does not contact a registry, resolve any root tag, or prove a mount is ready. Startup validates config/TLS and binds without contacting registries, so an absent or unavailable root can coexist with a healthy `/healthz` response.

For mount readiness, configure a known small marker file and probe that exact URL with the same TLS and caller credentials used by clients. A successful file GET/HEAD exercises the selected mount and registry path; failures map to the documented `404`, `502`, `503`, or `504` behavior. For an anonymous registry, the artifact CLI can also list a mount:

```sh
oci-auto-http artifact --config ./config.toml list /packages/
```

The artifact CLI has no Authorization option, so use an authenticated HTTPS request or a separately authenticated monitor for protected registries. Do not restart a service just because an upstream registry is temporarily unavailable: `/healthz` is intentionally independent of registry availability, and the service can recover when the registry returns.

Logs go to stderr (journald in the systemd example); choose text or JSON using `--log-format`. The service has no metrics endpoint. Do not add credentials, signed URLs, or secret-bearing query strings to monitoring output.

## Backups and recovery

The OCI registry is the durable store; the service keeps no local content catalog. Each mount's `root_tag` identifies the current root OCI index, which points to the reachable tree manifests and file blobs. A backup must preserve the root tag **and the complete reachable OCI manifest/blob graph**, including custom OCI media types, annotations, digests, and sizes. A copied tag without its referenced objects is not a backup. Prefer the registry vendor's repository replication/snapshot facility or an OCI graph-copy tool that explicitly supports nested OCI indexes and arbitrary artifact media types; validate the chosen tool before relying on it.

For a consistent backup, pause all writers to the `(registry, repository, root_tag)` while capturing/copying the repository. The server serializes writes only within one process. OCI Distribution does not provide portable tag compare-and-swap, so CLI writers and other service instances must be stopped or externally coordinated. Keep registry garbage collection from removing objects reachable from the backup root.

Practice recovery by restoring/copying the repository and root tag to a disposable repository, creating a temporary mount for it, then listing and reading known paths and comparing SHA-256 digests. Restore production data using registry-native graph/tag operations while writers are stopped, then verify known paths before resuming. The service does not have a local database or a command that reconstructs a lost root tag from unreachable manifests; retain registry-native backups of both the graph and tag. Local TOML config, TLS keys, and registry-side storage/backup policy need their own protected backups.

## Static package-repository examples

All examples below assume a mount prefix and registry repository have already been created, the registry is reachable, and the publisher has exclusive write access. Each CLI mutation publishes a new root tag; bulk import loops are easy to audit but can be slow for very large repositories. Serialize them and prefer a purpose-built importer for large snapshots. The HTTP directory autoindex is a generated listing, not an `index.html` fallback or a package-manager metadata generator.

### Python wheels with `pip --find-links`

A flat wheel directory works with `pip --find-links` because the generated autoindex contains ordinary HTML links. It is not a PEP 503 Simple API; do not configure this URL as `--index-url`.

```sh
oci-auto-http mount --config ./packages.toml create wheels \
  --prefix /wheels --registry https://registry.example.com \
  --repository team/wheels
for wheel in ./wheelhouse/*.whl; do
  [ -f "$wheel" ] || continue
  oci-auto-http artifact --config ./packages.toml create \
    "/wheels/$(basename "$wheel")" --input "$wheel"
done
python -m pip install --no-index \
  --find-links https://packages.example.test/wheels/ example-package
```

For a PEP 503 index, use a different server that implements its path/index semantics or generate and publish a compatible static index through a service that serves `index.html` as the directory document; this service intentionally has no implicit index-file precedence.

### RPM repository

Create a mount for an RPM repository, then generate package metadata and signatures with the normal RPM tooling. For example, after `createrepo_c ./repo/el9/x86_64`:

```sh
oci-auto-http mount --config ./packages.toml create rpm \
  --prefix /rpm --registry https://registry.example.com \
  --repository team/rpm
```

Publish the resulting files (including `repodata/repomd.xml`) beneath that mount:

```sh
find ./repo/el9/x86_64 -type f -print0 |
while IFS= read -r -d '' file; do
  relative=${file#./repo/el9/x86_64/}
  oci-auto-http artifact --config ./packages.toml create \
    "/rpm/el9/x86_64/$relative" --input "$file"
done
```

A client repo file can use:

```ini
[company-el9]
name=Company EL9 packages
baseurl=https://packages.example.test/rpm/el9/x86_64/
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.example.test/rpm/keys/RPM-GPG-KEY-company
```

Publish the public signing key and generated repodata; keep signing private keys outside the served tree. Clients must trust the HTTPS certificate and repository signing key.

### Debian/APT repository

Create a mount for the Debian repository, then generate it with Debian tooling such as `reprepro`/`aptly` and sign its Release metadata:

```sh
oci-auto-http mount --config ./packages.toml create debian \
  --prefix /debian --registry https://registry.example.com \
  --repository team/debian
```

Upload the generated `dists/` and `pool/` files while preserving their relative paths:

```sh
find ./apt-repo -type f -print0 |
while IFS= read -r -d '' file; do
  relative=${file#./apt-repo/}
  oci-auto-http artifact --config ./packages.toml create \
    "/debian/$relative" --input "$file"
done
```

Configure a client with a separately installed public archive key:

```text
deb [signed-by=/usr/share/keyrings/company-archive.gpg] https://packages.example.test/debian stable main
```

The repository must contain valid `InRelease` or `Release`/`Release.gpg`, package indexes, and pool paths produced by your repository tooling. Do not disable signature verification to compensate for a bad repository or TLS setup.
