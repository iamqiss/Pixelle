# Messenger Examples

This directory contains comprehensive sample applications that showcase various usage patterns of the Messenger client SDK for Python, from basic operations to advanced multi-tenant scenarios. To learn more about building applications with Messenger, please refer to the [getting started](https://messenger.apache.org/docs/introduction/getting-started) guide.

## Running Examples

To run any example, first start the server with

```bash
# Using latest release
docker run --rm -p 8080:8080 -p 3000:3000 -p 8090:8090 apache/messenger:latest

# Or build from source (recommended for development)
cd ../../ && cargo run --bin messenger-server
```

For server configuration options and help:

```bash
cargo run --bin messenger-server -- --help
```

You can also customize the server using environment variables:

```bash
## Example: Enable HTTP transport and set custom address
MESSENGER_HTTP_ENABLED=true MESSENGER_TCP_ADDRESS=0.0.0.0:8090 cargo run --bin messenger-server
```

and then install Python dependencies:

```bash
pip -r requirements.txt
```

## Basic Examples

### Getting Started

Perfect introduction for newcomers to Messenger:

```bash
python getting-started/producer.py
python getting-started/consumer.py
```

### Basic Usage

Core functionality with detailed configuration options:

```bash
python basic/producer.py <connection_string>
python basic/consumer.py <connection_string>
```

Demonstrates fundamental client connection, authentication, batch message sending, and polling with support for TCP/QUIC/HTTP protocols.
