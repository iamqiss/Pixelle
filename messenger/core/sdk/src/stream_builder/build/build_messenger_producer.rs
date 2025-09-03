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
use crate::clients::producer_config::DirectConfig;
use crate::prelude::{MessengerError, MessengerExpiry, MaxTopicSize};
use crate::stream_builder::MessengerProducerConfig;
use tracing::{error, trace};

/// Build a producer from the stream configuration.
///
/// # Arguments
///
/// * `client` - The Messenger client.
/// * `config` - The configuration.
///
/// # Errors
///
/// * `MessengerError` - If the messenger producer cannot be build.
///
/// # Details
///
/// This function will create a new `MessengerProducer` with the given `MessengerClient` and `MessengerProducerConfig`.
/// The `MessengerProducerConfig` fields are used to configure the `MessengerProducer`.
///
pub(crate) async fn build_messenger_producer(
    client: &MessengerClient,
    config: &MessengerProducerConfig,
) -> Result<MessengerProducer, MessengerError> {
    trace!("Extract config fields.");
    let stream = config.stream_name();
    let topic = config.topic_name();
    let topic_partitions_count = config.topic_partitions_count();
    let topic_replication_factor = config.topic_replication_factor();
    let batch_length = config.batch_length();
    let linger_time = config.linger_time();
    let partitioning = config.partitioning().to_owned();
    let send_retries = config.send_retries_count();
    let send_retries_interval = config.send_retries_interval();

    trace!("Build messenger producer");
    let mut builder = client
        .producer(stream, topic)?
        .partitioning(partitioning)
        .create_stream_if_not_exists()
        .send_retries(send_retries, send_retries_interval)
        .create_topic_if_not_exists(
            topic_partitions_count,
            topic_replication_factor,
            MessengerExpiry::ServerDefault,
            MaxTopicSize::ServerDefault,
        )
        .direct(
            DirectConfig::builder()
                .batch_length(batch_length)
                .linger_time(linger_time)
                .build(),
        );

    if let Some(encryptor) = config.encryptor() {
        builder = builder.encryptor(encryptor);
    }

    trace!("Initialize messenger producer");
    let producer = builder.build();
    producer.init().await.map_err(|err| {
        error!("Failed to initialize consumer: {err}");
        err
    })?;

    Ok(producer)
}
