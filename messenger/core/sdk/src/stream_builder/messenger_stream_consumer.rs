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

use crate::clients::client::MessengerClient;
use crate::clients::consumer::MessengerConsumer;
use crate::prelude::{MessengerError, SystemClient};
use crate::stream_builder::{MessengerConsumerConfig, build};
use tracing::trace;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct MessengerStreamConsumer;

impl MessengerStreamConsumer {
    /// Creates a new `MessengerStreamConsumer` with an existing client and `MessengerConsumerConfig`.
    ///
    /// # Arguments
    ///
    /// * `client`: the existing `MessengerClient` to use for the consumer.
    /// * `config`: the `MessengerConsumerConfig` to use to build the consumer.
    ///
    /// # Errors
    ///
    /// If the builds fails, an `MessengerError` is returned.
    ///
    pub async fn build(
        client: &MessengerClient,
        config: &MessengerConsumerConfig,
    ) -> Result<MessengerConsumer, MessengerError> {
        trace!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(MessengerError::NotConnected);
        }

        trace!("Check if stream and topic exist");
        build::build_messenger_stream_topic_if_not_exists(client, config).await?;

        trace!("Build messenger consumer");
        let messenger_consumer = build::build_messenger_consumer(client, config).await?;

        Ok(messenger_consumer)
    }

    /// Creates a new `MessengerStreamConsumer` by building a client from a connection string and
    /// a consumer with an `MessengerConsumerConfig`.
    ///
    /// # Arguments
    ///
    /// * `connection_string`: the connection string to use to build the client.
    /// * `config`: the `MessengerConsumerConfig` to use to build the consumer.
    ///
    /// # Errors
    ///
    /// If the builds fails, an `MessengerError` is returned.
    ///
    pub async fn with_client_from_url(
        connection_string: &str,
        config: &MessengerConsumerConfig,
    ) -> Result<(MessengerClient, MessengerConsumer), MessengerError> {
        trace!("Build and connect messenger client");
        let client = build::build_messenger_client(connection_string).await?;

        trace!("Check if stream and topic exist");
        build::build_messenger_stream_topic_if_not_exists(&client, config).await?;

        trace!("Build messenger consumer");
        let messenger_consumer = build::build_messenger_consumer(&client, config).await?;

        Ok((client, messenger_consumer))
    }

    /// Builds an `MessengerClient` from the given connection string.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - The connection string to use.
    ///
    /// # Errors
    ///
    /// * `MessengerError` - If the connection string is invalid or the client cannot be initialized.
    ///
    /// # Details
    ///
    /// This function will create a new `MessengerClient` with the given `connection_string`.
    /// It will then connect to the server using the provided connection string.
    /// If the connection string is invalid or the client cannot be initialized,
    /// an `MessengerError` will be returned.
    ///
    pub async fn build_messenger_client(connection_string: &str) -> Result<MessengerClient, MessengerError> {
        trace!("Build and connect messenger client");
        let client = build::build_messenger_client(connection_string).await?;

        Ok(client)
    }
}
