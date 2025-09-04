# syntax=docker/dockerfile:1

FROM rustlang/rust:nightly-slim AS builder
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

# Cache dependencies
COPY moderation/Cargo.toml ./moderation/Cargo.toml
COPY moderation/Cargo.lock ./moderation/Cargo.lock
RUN mkdir -p moderation/src && echo "fn main(){}" > moderation/src/main.rs && \
    cd moderation && cargo build --release || true

# Build actual binary
COPY moderation ./moderation
WORKDIR /app/moderation
RUN cargo build --release

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* && \
    useradd -m -u 10001 appuser
WORKDIR /app
COPY --from=builder /app/moderation/target/release/moderation /app/moderation
ENV RUST_LOG=info
EXPOSE 8080
USER appuser
CMD ["/app/moderation"]