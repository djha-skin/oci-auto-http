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

Use `--log-format text` (default) or `--log-format json` to select stderr logging. `RUST_LOG` controls the tracing filter; invalid filters fall back to `info`. Credentials and request data are not accepted as command-line arguments.

Run `oci-auto-http <command> --help` for command-specific usage. Invalid command syntax exits `2`; reserved commands exit `69` (`EX_UNAVAILABLE`) with a diagnostic and make no registry request.

## Behavior tests

Run `cargo test --locked` to exercise the compiled CLI and a real loopback HTTP listener; the tests need only the Rust toolchain and loopback sockets, not an OCI registry or external service. On Unix, the listener test also verifies graceful SIGTERM shutdown.
