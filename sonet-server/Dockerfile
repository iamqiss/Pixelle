#
# Copyright (c) 2025 Neo Qiss
# All rights reserved.
# 
# This software is proprietary and confidential.
# Unauthorized copying, distribution, or use is strictly prohibited.
#

# Multi-stage Dockerfile for Sonet microservices

# Build stage
FROM ubuntu:24.04 AS build

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    wget \
    python3 \
    python3-pip \
    pkg-config \
    libssl-dev \
    libpq-dev \
    postgresql-client \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Install Conan
RUN pip3 install conan==2.0.17

# Set up Conan profile
RUN conan profile detect --force

WORKDIR /app

# Copy dependency files
COPY conanfile.txt .
COPY CMakeLists.txt .

# Install dependencies
RUN conan install . --output-folder=build --build=missing -s build_type=Release

# Copy source code
COPY . .

# Build the project
RUN cd build && \
    cmake .. -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release && \
    cmake --build . -j$(nproc)

# Runtime stage
FROM ubuntu:24.04 AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl3 \
    libpq5 \
    postgresql-client \
    redis-tools \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r sonet && useradd -r -g sonet sonet

WORKDIR /app

# Copy built binaries
COPY --from=build /app/build/bin/* ./bin/
COPY --from=build /app/build/lib/* ./lib/

# Copy configuration files
COPY config/ ./config/
COPY database/ ./database/

# Set ownership
RUN chown -R sonet:sonet /app

USER sonet

EXPOSE 8080 9090

CMD ["./bin/gateway"]
