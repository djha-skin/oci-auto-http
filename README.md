# oci-auto-http

`oci-auto-http` is a Rust service foundation for serving generic byte trees stored in OCI registries. The product contracts are in [`docs/decisions/`](docs/decisions/).

## Build and run

```sh
cargo build --locked
cargo test --locked
cargo run -- --help
```

The current foundation exposes the stable command groups `serve`, `artifact`, and `mount`. Artifact and mount CRUD verbs are reserved in the CLI and return an actionable “not available yet” error until their storage features land; they do not contact a registry or create local artifact state.

Start the listener with:

```sh
cargo run -- serve --listen 127.0.0.1:8080
```

`GET` or `HEAD /healthz` returns `204 No Content`. Other paths return `503` until mount configuration and tree serving are implemented. The listener binds loopback by default. `Ctrl-C` and `SIGTERM` trigger graceful shutdown on Unix.

Use `--log-format text` (default) or `--log-format json` to select stderr logging. `RUST_LOG` controls the tracing filter; invalid filters fall back to `info`. Credentials and request data are not accepted as command-line arguments.

Run `oci-auto-http <command> --help` for command-specific usage. Invalid command syntax exits `2`; reserved commands exit `69` (`EX_UNAVAILABLE`) with a diagnostic and make no registry request.

## Behavior tests

Run `cargo test --locked` to exercise the compiled CLI and a real loopback HTTP listener; the tests need only the Rust toolchain and loopback sockets, not an OCI registry or external service. On Unix, the listener test also verifies graceful SIGTERM shutdown.
