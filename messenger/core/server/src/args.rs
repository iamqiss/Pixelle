/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    author = "Apache Messenger (Incubating)",
    version,
    about = "Apache Messenger: Hyper-Efficient Message Streaming at Laser Speed",
    long_about = r#"Apache Messenger (Incubating) - A persistent message streaming platform written in Rust

Apache Messenger is a high-performance message streaming platform that supports QUIC, TCP, and HTTP 
transport protocols, capable of processing millions of messages per second with low latency.

WEBSITE:
    https://messenger.apache.org

REPOSITORY:
    https://github.com/apache/messenger

DOCUMENTATION:
    https://messenger.apache.org/docs

CONFIGURATION:
    The server uses a TOML configuration file. By default, it looks for 'configs/server.toml' 
    in the current working directory. You can override this with the MESSENGER_CONFIG_PATH environment
    variable or use the --config-provider flag.

    Examples:
        messenger-server                                    # Uses default file provider (configs/server.toml)
        messenger-server --config-provider file             # Explicitly use file provider
        MESSENGER_CONFIG_PATH=custom.toml messenger-server       # Use custom config file path

ENVIRONMENT VARIABLES:
    Any configuration value can be overridden using environment variables with the MESSENGER_ prefix.
    Use underscores to separate nested configuration keys (e.g., MESSENGER_TCP_ADDRESS=127.0.0.1:8090).

    Common examples:
        MESSENGER_TCP_ADDRESS=0.0.0.0:8090                  # Override TCP server address
        MESSENGER_HTTP_ENABLED=true                         # Enable HTTP transport
        MESSENGER_SYSTEM_PATH=/data/messenger                    # Set data storage path
        MESSENGER_SYSTEM_LOGGING_LEVEL=debug                # Set log level to debug

TRANSPORT PROTOCOLS:
    - TCP (binary protocol): High-performance, low-latency (default: 127.0.0.1:8090)
    - QUIC: Modern UDP-based protocol with built-in encryption (default: 127.0.0.1:8080)
    - HTTP: RESTful API for web integration (default: 127.0.0.1:3000, disabled by default)

GETTING STARTED:
    1. Start the server: messenger-server
    2. Install CLI: cargo install messenger-cli
    3. Create a stream: messenger stream create my-stream
    4. Create a topic: messenger topic create my-stream my-topic 1 none
    5. Send messages: echo "Hello, Messenger!" | messenger message send my-stream my-topic

For more information, visit: https://messenger.apache.org/docs/introduction/getting-started/"#
)]
pub struct Args {
    /// Configuration provider type
    ///
    /// Currently only 'file' provider is supported, which loads configuration from a TOML file.
    /// The file path can be specified via MESSENGER_CONFIG_PATH environment variable.
    #[arg(short, long, default_value = "file", verbatim_doc_comment)]
    pub config_provider: String,

    /// Remove system path before starting (WARNING: THIS WILL DELETE ALL DATA!)
    ///
    /// This flag will completely remove the system data directory (local_data by default)
    /// before starting the server. Use this for clean development setups or testing.
    ///
    /// Examples:
    ///   messenger-server --fresh                          # Start with fresh data directory
    ///   messenger-server -f                               # Short form
    #[arg(short, long, default_value_t = false, verbatim_doc_comment)]
    pub fresh: bool,
}
