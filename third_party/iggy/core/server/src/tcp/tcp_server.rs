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

use crate::configs::tcp::TcpConfig;
use crate::streaming::systems::system::SharedSystem;
use crate::tcp::{tcp_listener, tcp_socket, tcp_tls_listener};
use std::net::SocketAddr;
use tracing::info;

/// Starts the TCP server.
/// Returns the address the server is listening on.
pub async fn start(config: TcpConfig, system: SharedSystem) -> SocketAddr {
    let server_name = if config.tls.enabled {
        "Iggy TCP TLS"
    } else {
        "Iggy TCP"
    };
    info!("Initializing {server_name} server...");
    let socket = tcp_socket::build(config.ipv6, config.socket);
    let addr = match config.tls.enabled {
        true => tcp_tls_listener::start(&config.address, config.tls, socket, system).await,
        false => tcp_listener::start(&config.address, socket, system).await,
    };
    info!("{server_name} server has started on: {:?}", addr);
    addr
}
