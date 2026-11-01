FROM rust:1.88-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock README.md LICENSE ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 oci-auto-http \
    && useradd --system --uid 10001 --gid 10001 --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin oci-auto-http
COPY --from=build /src/target/release/oci-auto-http /usr/local/bin/oci-auto-http
USER 10001:10001
WORKDIR /
EXPOSE 8443
ENTRYPOINT ["/usr/local/bin/oci-auto-http"]
CMD ["--help"]
