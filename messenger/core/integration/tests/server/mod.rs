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

mod cg;
mod general;
mod scenarios;
mod specific;

use integration::{
    http_client::HttpClientFactory,
    quic_client::QuicClientFactory,
    tcp_client::TcpClientFactory,
    test_server::{ClientFactory, TestServer, Transport},
};
use scenarios::{
    bench_scenario, consumer_group_join_scenario,
    consumer_group_with_multiple_clients_polling_messages_scenario,
    consumer_group_with_single_client_polling_messages_scenario, create_message_payload,
    message_headers_scenario, stream_size_validation_scenario, system_scenario, user_scenario,
};
use std::future::Future;
use std::pin::Pin;

type ScenarioFn = fn(&dyn ClientFactory) -> Pin<Box<dyn Future<Output = ()> + '_>>;

fn system_scenario() -> ScenarioFn {
    |factory| Box::pin(system_scenario::run(factory))
}

fn user_scenario() -> ScenarioFn {
    |factory| Box::pin(user_scenario::run(factory))
}

fn message_headers_scenario() -> ScenarioFn {
    |factory| Box::pin(message_headers_scenario::run(factory))
}

fn create_message_payload_scenario() -> ScenarioFn {
    |factory| Box::pin(create_message_payload::run(factory))
}

fn join_scenario() -> ScenarioFn {
    |factory| Box::pin(consumer_group_join_scenario::run(factory))
}

fn stream_size_validation_scenario() -> ScenarioFn {
    |factory| Box::pin(stream_size_validation_scenario::run(factory))
}

fn single_client_scenario() -> ScenarioFn {
    |factory| Box::pin(consumer_group_with_single_client_polling_messages_scenario::run(factory))
}

fn multiple_clients_scenario() -> ScenarioFn {
    |factory| Box::pin(consumer_group_with_multiple_clients_polling_messages_scenario::run(factory))
}

fn bench_scenario() -> ScenarioFn {
    |factory| Box::pin(bench_scenario::run(factory))
}

async fn run_scenario(transport: Transport, scenario: ScenarioFn) {
    let mut test_server = TestServer::default();
    test_server.start();

    let client_factory: Box<dyn ClientFactory> = match transport {
        Transport::Tcp => {
            let server_addr = test_server.get_raw_tcp_addr().unwrap();
            Box::new(TcpClientFactory {
                server_addr,
                ..Default::default()
            })
        }
        Transport::Quic => {
            let server_addr = test_server.get_quic_udp_addr().unwrap();
            Box::new(QuicClientFactory { server_addr })
        }
        Transport::Http => {
            let server_addr = test_server.get_http_api_addr().unwrap();
            Box::new(HttpClientFactory { server_addr })
        }
    };

    scenario(&*client_factory).await;
}
