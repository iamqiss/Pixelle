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

use crate::clients::client_builder::MessengerClientBuilder;
use messenger_common::locking::{MessengerSharedMut, MessengerSharedMutFn};

use crate::client_wrappers::client_wrapper::ClientWrapper;
use crate::http::http_client::HttpClient;
use crate::prelude::EncryptorKind;
use crate::prelude::MessengerConsumerBuilder;
use crate::prelude::MessengerError;
use crate::prelude::MessengerProducerBuilder;
use crate::quic::quic_client::QuicClient;
use crate::tcp::tcp_client::TcpClient;
use async_broadcast::Receiver;
use async_trait::async_trait;
use messenger_binary_protocol::{Client, SystemClient};
use messenger_common::{
    ConnectionStringUtils, Consumer, DiagnosticEvent, Partitioner, TransportProtocol,
};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::spawn;
use tokio::time::sleep;
use tracing::log::warn;
use tracing::{debug, error, info};

/// The main client struct which implements all the `Client` traits and wraps the underlying low-level client for the specific transport.
///
/// It also provides the additional builders for the standalone consumer, consumer group, and producer.
#[derive(Debug)]
#[allow(dead_code)]
pub struct MessengerClient {
    pub(crate) client: MessengerSharedMut<ClientWrapper>,
    partitioner: Option<Arc<dyn Partitioner>>,
    pub(crate) encryptor: Option<Arc<EncryptorKind>>,
}

impl Default for MessengerClient {
    fn default() -> Self {
        MessengerClient::new(ClientWrapper::Tcp(TcpClient::default()))
    }
}

impl MessengerClient {
    /// Creates a new `MessengerClientBuilder`.
    pub fn builder() -> MessengerClientBuilder {
        MessengerClientBuilder::new()
    }

    /// Creates a new `MessengerClientBuilder` from the provided connection string.
    pub fn builder_from_connection_string(
        connection_string: &str,
    ) -> Result<MessengerClientBuilder, MessengerError> {
        MessengerClientBuilder::from_connection_string(connection_string)
    }

    /// Creates a new `MessengerClient` with the provided client implementation for the specific transport.
    pub fn new(client: ClientWrapper) -> Self {
        let client = MessengerSharedMut::new(client);
        MessengerClient {
            client,
            partitioner: None,
            encryptor: None,
        }
    }

    /// Creates a new `MessengerClient` from the provided connection string.
    pub fn from_connection_string(connection_string: &str) -> Result<Self, MessengerError> {
        match ConnectionStringUtils::parse_protocol(connection_string)? {
            TransportProtocol::Tcp => Ok(MessengerClient::new(ClientWrapper::Tcp(
                TcpClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::Quic => Ok(MessengerClient::new(ClientWrapper::Quic(
                QuicClient::from_connection_string(connection_string)?,
            ))),
            TransportProtocol::Http => Ok(MessengerClient::new(ClientWrapper::Http(
                HttpClient::from_connection_string(connection_string)?,
            ))),
        }
    }

    /// Creates a new `MessengerClient` with the provided client implementation for the specific transport and the optional implementations for the `partitioner` and `encryptor`.
    pub fn create(
        client: ClientWrapper,
        partitioner: Option<Arc<dyn Partitioner>>,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> Self {
        if partitioner.is_some() {
            info!("Partitioner is enabled.");
        }
        if encryptor.is_some() {
            info!("Client-side encryption is enabled.");
        }

        let client = MessengerSharedMut::new(client);
        MessengerClient {
            client,
            partitioner,
            encryptor,
        }
    }

    /// Returns the underlying client implementation for the specific transport.
    pub fn client(&self) -> MessengerSharedMut<ClientWrapper> {
        self.client.clone()
    }

    /// Returns the builder for the standalone consumer.
    pub fn consumer(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Result<MessengerConsumerBuilder, MessengerError> {
        Ok(MessengerConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::new(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            Some(partition),
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the builder for the consumer group.
    pub fn consumer_group(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
    ) -> Result<MessengerConsumerBuilder, MessengerError> {
        Ok(MessengerConsumerBuilder::new(
            self.client.clone(),
            name.to_owned(),
            Consumer::group(name.try_into()?),
            stream.try_into()?,
            topic.try_into()?,
            None,
            self.encryptor.clone(),
            None,
        ))
    }

    /// Returns the builder for the producer.
    pub fn producer(&self, stream: &str, topic: &str) -> Result<MessengerProducerBuilder, MessengerError> {
        Ok(MessengerProducerBuilder::new(
            self.client.clone(),
            stream.try_into()?,
            stream.to_owned(),
            topic.try_into()?,
            topic.to_owned(),
            self.encryptor.clone(),
            None,
        ))
    }
}

#[async_trait]
impl Client for MessengerClient {
    async fn connect(&self) -> Result<(), MessengerError> {
        let heartbeat_interval;
        {
            let client = self.client.read().await;
            client.connect().await?;
            heartbeat_interval = client.heartbeat_interval().await;
        }

        let client = self.client.clone();
        spawn(async move {
            loop {
                debug!("Sending the heartbeat...");
                if let Err(error) = client.read().await.ping().await {
                    error!("There was an error when sending a heartbeat. {error}");
                    if error == MessengerError::ClientShutdown {
                        warn!("The client has been shut down - stopping the heartbeat.");
                        return;
                    }
                } else {
                    debug!("Heartbeat was sent successfully.");
                }
                sleep(heartbeat_interval.get_duration()).await
            }
        });
        Ok(())
    }

    async fn disconnect(&self) -> Result<(), MessengerError> {
        self.client.read().await.disconnect().await
    }

    async fn shutdown(&self) -> Result<(), MessengerError> {
        self.client.read().await.shutdown().await
    }

    async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
        self.client.read().await.subscribe_events().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_fail_with_empty_connection_string() {
        let value = "";
        let client = MessengerClient::from_connection_string(value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_username() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_password() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_server_address() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_without_port() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_fail_with_invalid_prefix() {
        let connection_string_prefix = "invalid+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_err());
    }

    #[test]
    fn should_succeed_with_default_prefix() {
        let default_connection_string_prefix = "messenger://";
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{default_connection_string_prefix}{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_tcp_protocol() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_tcp_protocol_using_pat() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Tcp;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "messengerpat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn should_succeed_with_quic_protocol() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn should_succeed_with_quic_protocol_using_pat() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Quic;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "messengerpat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_http_protocol() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let username = "user";
        let password = "secret";
        let value = format!(
            "{connection_string_prefix}{protocol}://{username}:{password}@{server_address}:{port}"
        );
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_ok());
    }

    #[test]
    fn should_succeed_with_http_protocol_with_pat() {
        let connection_string_prefix = "messenger+";
        let protocol = TransportProtocol::Http;
        let server_address = "127.0.0.1";
        let port = "1234";
        let pat = "messengerpat-1234567890abcdef";
        let value = format!("{connection_string_prefix}{protocol}://{pat}@{server_address}:{port}");
        let client = MessengerClient::from_connection_string(&value);
        assert!(client.is_ok());
    }
}
