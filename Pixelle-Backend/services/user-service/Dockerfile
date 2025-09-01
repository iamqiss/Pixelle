FROM rust:1.75-slim as builder

WORKDIR /app
COPY . .

RUN cargo build --release --package pixelle-user-service

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/pixelle-user-service .

EXPOSE 8081

CMD ["./pixelle-user-service"]
