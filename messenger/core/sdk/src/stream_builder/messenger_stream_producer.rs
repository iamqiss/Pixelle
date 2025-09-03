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
use crate::clients::producer::MessengerProducer;
use crate::prelude::{MessengerError, SystemClient};
use crate::stream_builder::{MessengerProducerConfig, build};
use tracing::trace;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct MessengerStreamProducer;

impl MessengerStreamProducer {
    /// Creates a new `MessengerProducer` instance and its associated producer using the `client` and
    /// `config` parameters.
    ///
    /// # Arguments
    ///
    /// * `client`: The Messenger client to use to connect to the Messenger server.
    /// * `config`: The configuration for the producer.
    ///
    /// # Errors
    ///
    /// If the client is not connected or the producer cannot be built, an `MessengerError` is returned.
    ///
    pub async fn build(
        client: &MessengerClient,
        config: &MessengerProducerConfig,
    ) -> Result<MessengerProducer, MessengerError> {
        trace!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(MessengerError::NotConnected);
        }

        trace!("Build messenger producer");
        // The producer creates stream and topic if it doesn't exist
        let messenger_producer = build::build_messenger_producer(client, config).await?;

        Ok(messenger_producer)
    }

    /// Creates a new `MessengerStreamProducer` instance and its associated client using the `connection_string`
    /// and `config` parameters.
    ///
    /// # Arguments
    ///
    /// * `connection_string`: The connection string to use to connect to the Messenger server.
    /// * `config`: The configuration for the producer.
    ///
    /// # Errors
    ///
    /// If the client cannot be connected or the producer cannot be built, an `MessengerError` is returned.
    ///
    pub async fn with_client_from_url(
        connection_string: &str,
        config: &MessengerProducerConfig,
    ) -> Result<(MessengerClient, MessengerProducer), MessengerError> {
        trace!("Build and connect messenger client");
        let client = build::build_messenger_client::build_messenger_client(connection_string).await?;

        trace!("Build messenger producer");
        // The producer creates stream and topic if it doesn't exist
        let messenger_producer = build::build_messenger_producer(&client, config).await?;

        Ok((client, messenger_producer))
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
