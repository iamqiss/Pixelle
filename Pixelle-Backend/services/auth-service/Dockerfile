# Multi-stage build for Pixelle Auth Service
FROM rust:1.75-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY services/auth-service/ ./services/auth-service/

# Build the application
RUN cd services/auth-service && \
    cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r pixelle && useradd -r -g pixelle pixelle

# Set working directory
WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/services/auth-service/target/release/pixelle-auth-service /app/

# Change ownership to non-root user
RUN chown -R pixelle:pixelle /app

# Switch to non-root user
USER pixelle

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set environment variables
ENV RUST_LOG=info
ENV JWT_SECRET=change-me-in-production
ENV JWT_EXPIRATION_SECONDS=3600
ENV REFRESH_TOKEN_EXPIRATION_SECONDS=604800

# Run the application
CMD ["./pixelle-auth-service"]