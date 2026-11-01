# oci-auto-http

`oci-auto-http` is a Rust service foundation for serving generic byte trees stored in OCI registries. The product contracts are in [`docs/decisions/`](docs/decisions/).

## Build and run

```sh
cargo build --locked
cargo test --locked
cargo run -- --help
```

The current foundation exposes the stable command groups `serve`, `artifact`, and `mount`. Artifact and mount CRUD verbs are reserved in the CLI and return an actionable “not available yet” error until their storage features land; they do not contact a registry or create local artifact state.

Start the listener with a TOML configuration file:

```sh
cargo run -- serve --config config.example.toml
```

`--listen <IP:PORT>` explicitly overrides `[server].listen`; otherwise the config's safe loopback default is used. The checked-in [`config.example.toml`](config.example.toml) is a template and its registry host/repository are placeholders. Configuration is validated before the listener binds; this foundation does not contact registries during startup. `GET` or `HEAD /healthz` returns `204 No Content`. Other paths return `503` until mount routing and tree serving are implemented. `Ctrl-C` and `SIGTERM` trigger graceful shutdown on Unix.

## Configuration

Configuration is version-1 TOML. `version` is required; `[server].listen` defaults to `127.0.0.1:8080`. Each `[[mounts]]` entry requires a unique lowercase alphanumeric/hyphen `name`, a canonical absolute URI `prefix`, an HTTPS registry origin without userinfo/path/query/fragment, and an OCI repository name. Prefixes are decoded once, component-boundary matched, and must not overlap. `root_tag` defaults to `oci-auto-http-tree-v1`. `redirect_allowlist` contains exact HTTPS authorities (`host:port`, default port 443) for permitted blob redirects. Unknown fields and unsupported versions fail before listening. Registry credentials are never placed in this file; the HTTP service forwards only the request caller's Authorization value under the separate trust contract.

The package requires Rust 1.88 or newer.

## OCI Distribution client library

The crate exposes `oci_auto_http::registry::RegistryClient` for repository-scoped OCI Distribution v2 operations. Construct a validated `config::Mount` through `config::Config::load` or `Config::parse`, then create the client with `RegistryClient::new(mount, ClientPolicy::default())`. The boundary provides tag resolution, manifest GET/PUT/DELETE, blob HEAD/streaming GET/PUT/DELETE, and bounded paginated tag listing. It supports OCI indexes and image/artifact manifests through their media types; it does not call `_catalog` or infer tree paths from registry names.

The module re-exports `Bytes`, `Stream`, and `HeaderValue` for callers of these APIs. Pass an optional `registry::HeaderValue` Authorization value to an operation. It is sent only to the configured HTTPS registry origin and is never forwarded to an allowlisted blob redirect. Upstream 401 challenges and `Retry-After` are exposed on `RegistryError` for exact HTTP relay, while its `Display` and `Debug` output omit headers, URLs, and response bodies. Blob GET returns a streaming body that checks the descriptor digest and size when fully consumed; blob uploads stream through an OCI PATCH session, verify the declared digest/size, then finalize with a separate PUT. Buffered manifests and tag pages are bounded (1 MiB by default, configurable up to 16 MiB); GET/HEAD use at most two retries by default, all writes are non-retryable, requests time out after 30 seconds, and blob redirects are manually constrained to HTTPS and exact configured authorities. Private-registry CA certificates can be supplied through `ClientPolicy::with_root_certificate_pem`.

This library boundary is not yet wired into the `serve`, `artifact`, or `mount` commands; those integrations follow in the storage and routing work. The currently implemented listener still returns `503` for mounted paths.

Use `--log-format text` (default) or `--log-format json` to select stderr logging. `RUST_LOG` controls the tracing filter; invalid filters fall back to `info`. Credentials and request data are not accepted as command-line arguments.

Run `oci-auto-http <command> --help` for command-specific usage. Invalid command syntax exits `2`; reserved commands exit `69` (`EX_UNAVAILABLE`) with a diagnostic and make no registry request.

## Behavior tests

Run `cargo test --locked --offline` for the compiled CLI and real loopback HTTP listener tests. They need only the Rust toolchain and loopback sockets. On Unix, the listener test verifies graceful SIGTERM shutdown.

The OCI Distribution integration suite is an explicit opt-in because it creates local Docker containers and transfers a 128 MiB blob to test an interrupted read. Pull the image once, then run:

```sh
docker pull registry:2
cargo test --locked --offline --features registry-tests --test registry_client -- --nocapture
```

The opt-in suite requires a running Docker daemon, OpenSSL, and curl. It starts isolated TLS-enabled `registry:2` containers on random loopback ports, uses a generated test CA and temporary registry storage/auth file, and removes containers/files on completion. It exercises actual manifest/blob publish, independent curl reads, digest/size verification, pagination-compatible tag listing, large tag-list reads, Basic challenge relay, 404, timeout, deletion, and an interrupted blob transfer; pure unit tests separately validate same-origin `Link` pagination handling; it makes no calls to an external registry after the image is present locally.
