# oci-auto-http

`oci-auto-http` is a Rust service foundation for serving generic byte trees stored in OCI registries. The product contracts are in [`docs/decisions/`](docs/decisions/).

## Build and run

```sh
cargo build --locked
cargo test --locked
cargo run -- --help
```

The application exposes the stable command groups `serve`, `artifact`, and `mount`. Artifact CRUD commands operate on OCI-backed tree paths; mount CRUD commands manage the version-1 TOML configuration without contacting a registry.

Start the listener with a TOML configuration file:

```sh
cargo run -- serve --config config.example.toml
# For caller Authorization pass-through, add --tls-cert ./server-chain.pem --tls-key ./server-key.pem.
# Add --ca-cert ./registry-ca.pem separately when the upstream registry uses a private CA.
```

`--listen <IP:PORT>` explicitly overrides `[server].listen`; otherwise the config's safe loopback default is used. `--tls-cert FILE --tls-key FILE` enables a direct TLS listener using a PEM certificate chain and matching PEM private key; both are required together. This mode serves HTTP/1.1 over TLS and does not request client certificates. `--ca-cert FILE` separately adds a PEM trust root for upstream registries. The checked-in [`config.example.toml`](config.example.toml) is a template and its registry host/repository are placeholders. Configuration, optional listener TLS material, and OCI clients are validated before the listener binds; startup does not contact registries. `GET` or `HEAD /healthz` returns `204 No Content`. Requests outside configured mounts return `404`; absent roots, registry transport/server failures, and rate limits return `503` (a valid registry `Retry-After` is preserved); upstream timeouts return `504`, and corrupt/protocol failures return `502`. Mounted files are routed to OCI and streamed with digest verification, MIME types, ETags, conditional requests, and single byte ranges. Known directories redirect to a canonical trailing-slash URL; slash-terminated directories return generated HTML5 listings. `Ctrl-C` and `SIGTERM` trigger graceful shutdown on Unix.

## Configuration

Configuration is version-1 TOML. `version` is required; `[server].listen` defaults to `127.0.0.1:8080`. Each `[[mounts]]` entry requires a unique lowercase alphanumeric/hyphen `name`, a canonical absolute URI `prefix`, an HTTPS registry origin without userinfo/path/query/fragment, and an OCI repository name. Prefixes are decoded once, component-boundary matched, and must not overlap. `root_tag` defaults to `oci-auto-http-tree-v1`. `redirect_allowlist` contains exact HTTPS authorities (`host:port`, default port 443) for permitted blob redirects. Unknown fields and unsupported versions fail before listening. Registry credentials are never placed in this file. By default, `serve` listens on cleartext HTTP for anonymous mounts only; a single incoming `Authorization` field is rejected with `403`. Enable direct inbound HTTPS with the paired `--tls-cert FILE --tls-key FILE` options to allow credential pass-through. The service does not trust `Forwarded` or `X-Forwarded-Proto`; TLS termination at a proxy is not supported for authenticated requests. Upstream registry TLS trust remains separately configured with `--ca-cert`.

`[server]` also supports `max_concurrent_requests` (default `64`), `request_timeout_seconds` (`300`), `registry_request_timeout_seconds` (`30`), `stream_idle_timeout_seconds` (`30`), `max_autoindex_spool_bytes` (`16777216`), and `max_autoindex_spool_total_bytes` (`268435456`). Matching `serve --...` flags override TOML values. Saturation fails immediately with generic `503`; the request permit is held through response completion or disconnect. A total request or idle timeout before headers returns `504`; after headers, a timed-out body is terminated as incomplete. Autoindex preflight that exceeds either spool limit returns `503` before headers. These bounds are a starting point; do not expose beyond loopback until the documented P2 load/resilience criteria are satisfied.

## Mount commands

Mount commands read and validate the selected TOML file, then print JSON to standard output. `create` initializes a missing config file with version 1; `list` prints a JSON array, `show` prints one mount, and successful mutations print the affected mount or deleted name. Configuration edits preserve unrelated TOML comments/formatting, validate the complete result before writing, and atomically replace a regular non-symlink file. CLI mutations serialize through a persistent hidden lock file beside the config; do not remove it while commands may be running, and coordinate edits from external tools. A running `serve` process keeps the configuration loaded at startup, so restart it for mount changes to take effect. Mount commands never contact a registry. The final mount cannot be deleted, and a mount's name is immutable.

```sh
cargo run -- mount --config ./config.toml create packages \
  --prefix /packages --registry https://registry.example --repository team/packages
cargo run -- mount --config ./config.toml list
cargo run -- mount --config ./config.toml show packages
cargo run -- mount --config ./config.toml update packages --prefix /releases --root-tag releases-v2
cargo run -- mount --config ./config.toml update packages --reset-root-tag --clear-redirect-allowlist
cargo run -- mount --config ./config.toml delete packages
```

`update` changes only fields supplied: `--prefix`, `--registry`, `--repository`, `--root-tag`, and repeatable `--redirect-allowlist AUTHORITY`. Use `--reset-root-tag` to restore the default root tag or `--clear-redirect-allowlist` to remove redirect exceptions. Mount names follow the configuration grammar; invalid or overlapping mount definitions are rejected before the file is changed.

The package requires Rust 1.88 or newer.

## OCI Distribution client library

The crate exposes `oci_auto_http::registry::RegistryClient` for repository-scoped OCI Distribution v2 operations. Construct a validated `config::Mount` through `config::Config::load` or `Config::parse`, then create the client with `RegistryClient::new(mount, ClientPolicy::default())`. The boundary provides tag resolution, manifest GET/PUT/DELETE, blob HEAD/streaming GET/PUT/DELETE, and bounded paginated tag listing. It supports OCI indexes and image/artifact manifests through their media types; it does not call `_catalog` or infer tree paths from registry names.

The module re-exports `Bytes`, `Stream`, and `HeaderValue` for callers of these APIs. Pass an optional `registry::HeaderValue` Authorization value to an operation. It is sent only to the configured HTTPS registry origin and is never forwarded to an allowlisted blob redirect. Configured-registry 401 challenges and `Retry-After` are exposed on `RegistryError` for exact HTTP relay; responses from allowlisted blob stores suppress those relay fields. Error `Display` and `Debug` omit headers, URLs, and response bodies. Blob GET returns a streaming body that checks the descriptor digest and size when fully consumed; blob uploads stream through an OCI PATCH session, verify the declared digest/size, then finalize with a separate PUT. Buffered manifests and tag pages are bounded (1 MiB by default, configurable up to 16 MiB); GET/HEAD use at most two retries by default, all writes are non-retryable, requests time out after 30 seconds, and blob redirects are manually constrained to HTTPS and exact configured authorities. Private-registry CA certificates can be supplied through `ClientPolicy::with_root_certificate_pem`.

The OCI client and tree library are wired into the `artifact` commands and `serve`; the compiled mount/artifact/serve flow is exercised against a real local registry by the opt-in integration suite. `serve` supports read-only GET/HEAD file and directory paths. Caller Authorization is forwarded unchanged only to the selected HTTPS registry when the incoming listener is direct TLS; registry challenges are relayed only after validation and never over cleartext. Artifact CLI registry requests remain anonymous in this release.

## HTTP file responses

File responses use a built-in MIME map (unknown extensions are `application/octet-stream`), a strong quoted ETag derived from the SHA-256 layer digest, `Content-Length`, `Accept-Ranges: bytes`, `X-Content-Type-Options: nosniff`, and `Cache-Control: private, no-store`. Directory requests return deterministic HTML5 autoindexes with a parent link, directories before files, bytewise ordering, escaped names, and no inferred timestamps/sizes or implicit index-document precedence; explicit Accept exclusions return `406`. `If-Match` uses strong comparison; `If-None-Match` uses weak comparison; date validators are ignored because the tree stores no modification time. One satisfiable byte range returns `206` and `Content-Range`; unsatisfiable ranges return `416`; malformed or multiple ranges are ignored. `If-Range` accepts a matching strong ETag only. HEAD mirrors the GET status and representation headers without downloading file bytes. GET and range bodies stream with bounded memory and withhold the final response byte until the complete OCI digest and size verify. Directory URLs without a trailing slash redirect to the canonical URL; autoindex HTML uses a restrictive CSP and no-referrer policy. A directory GET preflights all registry-backed pages into an unnamed private temporary file before sending headers, so a later registry authentication failure can still produce the correct status and challenge; the generated listing is then streamed with bounded memory and the spool is discarded when the response ends.

## Artifact commands

Every artifact command requires `--config FILE`. Paths are absolute served URL paths, matched against a configured mount prefix and percent-decoded exactly once (`+` remains a literal plus). For example:

```sh
cargo run -- artifact --config ./config.toml create /packages/hello.txt --input ./hello.txt
cargo run -- artifact --config ./config.toml read /packages/hello.txt --output ./downloaded.txt
cargo run -- artifact --config ./config.toml list /packages
cargo run -- artifact --config ./config.toml update /packages/hello.txt --input ./replacement.txt
cargo run -- artifact --config ./config.toml delete /packages/hello.txt
```

`create` refuses to replace an existing path and creates missing parent directories; use `--directory` to create an empty directory. `--input FILE` selects file input; omit it or pass `-` to read standard input. `update` replaces an existing file only. `read` writes to standard output by default or atomically replaces `--output FILE` only after the streamed blob digest and size verify. `list` emits newline-delimited JSON: a listing header with the pinned root digest followed by entry records. It follows internal continuation cursors to completion within one invocation, so the process-local cursor key never needs to persist across CLI runs. Its registry page size defaults to 1000 and is configurable with `--limit`. When the configuration has multiple mounts, pass a path to identify the mount. `delete --recursive` is required for non-empty directories.

Input and output are streamed with bounded memory; standard input is staged in a temporary file so its digest and size can be verified before upload. `--ca-cert FILE` supplies an additional PEM CA certificate for a private registry. Artifact commands currently use anonymous registry access; `--ca-cert` configures upstream TLS trust, not registry credentials. Mutations to the same tree from separate CLI processes or services must be externally serialized; OCI tag updates do not provide portable compare-and-swap.

Stable exit codes: `2` usage/config/path errors, `65` integrity/content validation, `66` missing paths or mounts, `70` internal errors, `73` conflicts (including duplicate mounts or deleting the final mount), `74` registry/I/O failures, `75` stale cursors, uncertain commits, or concurrent config edits, `77` authorization failures, and `78` invalid mount targets.

## OCI tree storage library

`oci_auto_http::tree::TreeStore` implements the version-1 OCI tree format over a validated `RegistryClient`. Call `initialize` to create the empty root explicitly; the first file or directory creation also initializes it, while reads and listings never initialize or repair an absent/corrupt tree. `stat_file` resolves verified file metadata without downloading the data layer, while `read_file` returns a digest/size-verified stream. `write_file` is a byte-buffer convenience API; `write_file_stream` accepts a byte stream plus its expected digest/size for bounded-memory uploads and verifies the stream even on a no-op. Both create missing parent directories and can replace a file; identical content is a no-op that preserves the root digest. `create_directory` makes an empty directory. `remove(path, recursive, auth)` deletes a file or directory in one root publication; nonempty directories require `recursive = true`. Deleted manifests and blobs are not removed from the registry because they may be shared; they become unreachable from the root.

Library callers can use `StoragePath::parse_relative` for already-decoded relative paths or `StoragePath::from_components` after decoding request components exactly once. `list_page` accepts limits from 1 through 10,000, returns sorted immediate children, and issues opaque continuation tokens bound to the pinned root digest, directory, limit, and last name. Tokens are HMAC-protected using a process-local key, so they are valid only while that process remains running. Tree errors distinguish invalid paths, missing roots, missing entries, integrity failures, invalid/stale cursors, and ambiguous root publication. A process-local per-target lock serializes writers; external writers still require coordination or read-only operation.

Use `--log-format text` (default) or `--log-format json` to select stderr logging. `RUST_LOG` controls the tracing filter; invalid filters fall back to `info`. Credentials and request data are not accepted as command-line arguments.

Run `oci-auto-http <command> --help` for command-specific usage. Invalid command syntax exits `2`; mount CRUD only edits local configuration and makes no registry request.

## Behavior tests

Run `cargo test --locked --offline` for the compiled CLI and real loopback HTTP listener tests. They need only the Rust toolchain and loopback sockets. On Unix, the listener test verifies graceful SIGTERM shutdown.

The OCI Distribution and tree-storage integration suite is an explicit opt-in because it creates local Docker containers, injects commit-boundary connection drops through a local TLS proxy, and transfers a 128 MiB blob to test an interrupted read. Pull the image once, then run:

```sh
docker pull registry:2
cargo test --locked --offline --features registry-tests --test registry_client -- --nocapture
```

The opt-in suite requires a running Docker daemon, the local `registry:2` image, OpenSSL, and curl. It starts isolated TLS-enabled registries on random loopback ports, uses a generated test CA and temporary registry storage/auth file, and removes containers/files on completion. In addition to Distribution client coverage, it exercises nested tree writes and byte-exact reads across fresh client/store construction, independent curl inspection of root and file OCI objects, 129-entry branch-page bounds and ordering, concurrent writers, cursor snapshot pinning, source-stream failure before publication, and TLS-proxy connection drops both before and after the root-tag commit. It also invokes the compiled CLI against a real registry for artifact create from a file and stdin, stdout/file reads, nested paginated NDJSON listings, replacement, file/directory deletion, and conflict/missing-path errors. A complete user-facing flow creates a mount, publishes/reads/lists/deletes an artifact, and verifies GET/HEAD, autoindex, and post-delete 404 through a compiled `serve` process against that same real registry; an independent curl read checks exact OCI bytes and the test confirms no local durable content catalog is created. A compiled `serve` process is tested over real HTTP against two repositories in the local registry: exact GET/HEAD bytes, encoded paths, mount isolation (including registry request-log checks), directory HTML/redirects, missing roots without initialization, malformed/traversal paths, unsupported methods, and cleartext Authorization rejection. An auth-specific vector starts two htpasswd-protected registries and the compiled service with direct TLS, then verifies Basic challenge relay, wrong-credential rejection, successful access on each selected mount only, duplicate-header rejection, cleartext credential/challenge suppression, and secret-free service logs. A fault-injected registry returns a valid 401 challenge on a later page of a 129-entry autoindex; the real HTTPS client still receives 401 before any successful representation headers are committed. Resource tests hold a 32 MiB response open to prove request saturation, reject excess work before registry access, and verify permit recovery after disconnect/body completion; delayed upstream and response streams exercise total, per-registry, and idle timeouts, while small spool budgets fail closed before listing headers.
 HTTP file tests cover MIME types, ETags/conditional requests, ranges, empty files, and a 64 MiB streamed response with server RSS sampling on Linux; a streaming TLS proxy verifies upstream transfer cancellation after client disconnect and corrupts one real blob byte to prove the final response byte is withheld; stopping the real registry verifies upstream-failure mapping. The fault proxy forwards to a real local registry and only injects the specified dropped connection; happy paths are never mocked. Pure unit tests separately validate same-origin `Link` pagination; no calls are made to an external registry once the image is present locally.
