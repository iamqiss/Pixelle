FROM rust:1.75-slim as builder

WORKDIR /app
COPY . .

RUN cargo build --release --package pixelle-api-gateway

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/pixelle-api-gateway .

EXPOSE 8080

CMD ["./pixelle-api-gateway"]
