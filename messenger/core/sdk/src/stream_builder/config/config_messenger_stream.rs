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

use crate::prelude::{Identifier, MessengerDuration, MessengerError};
use crate::stream_builder::{MessengerConsumerConfig, MessengerProducerConfig};
use bon::Builder;

#[derive(Builder, Default, Debug, Clone)]
pub struct MessengerStreamConfig {
    consumer_config: MessengerConsumerConfig,
    producer_config: MessengerProducerConfig,
}

impl MessengerStreamConfig {
    /// Creates a new `MessengerStreamConfig` with the given consumer and producer configurations.
    ///
    /// # Args
    ///
    /// * `consumer_config` - The consumer configuration.
    /// * `producer_config` - The producer configuration.
    ///
    /// Returns:
    /// A new `MessengerStreamConfig`.
    ///
    pub fn new(consumer_config: MessengerConsumerConfig, producer_config: MessengerProducerConfig) -> Self {
        Self {
            consumer_config,
            producer_config,
        }
    }

    /// Creates a new `MessengerStreamConfig` from the given stream and topic names, along with the max
    /// batch size, the send interval and the polling interval.
    ///
    /// # Args
    ///
    /// * `stream` - The stream name.
    /// * `topic` - The topic name.
    /// * `batch_length` - The max number of messages to send in a batch.
    /// * `linger_time` - The interval between messages sent.
    /// * `polling_interval` - The interval between polling for new messages.
    ///
    /// Returns:
    /// A new `MessengerStreamConfig`.
    ///
    pub fn from_stream_topic(
        stream: &str,
        topic: &str,
        batch_length: u32,
        linger_time: MessengerDuration,
        polling_interval: MessengerDuration,
    ) -> Result<Self, MessengerError> {
        let consumer_config =
            MessengerConsumerConfig::from_stream_topic(stream, topic, batch_length, polling_interval)?;

        let producer_config =
            MessengerProducerConfig::from_stream_topic(stream, topic, batch_length, linger_time)?;

        Ok(Self {
            consumer_config,
            producer_config,
        })
    }
}

impl MessengerStreamConfig {
    pub fn consumer_config(&self) -> &MessengerConsumerConfig {
        &self.consumer_config
    }

    pub fn producer_config(&self) -> &MessengerProducerConfig {
        &self.producer_config
    }

    pub fn stream_id(&self) -> &Identifier {
        self.producer_config.stream_id()
    }

    pub fn stream_name(&self) -> &str {
        self.producer_config.stream_name()
    }

    pub fn topic_id(&self) -> &Identifier {
        self.producer_config.topic_id()
    }

    pub fn topic_name(&self) -> &str {
        self.producer_config.topic_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn should_be_equal() {
        let consumer_config = MessengerConsumerConfig::from_stream_topic(
            "test_stream",
            "test_topic",
            100,
            MessengerDuration::from_str("5ms").unwrap(),
        )
        .unwrap();

        let producer_config = MessengerProducerConfig::from_stream_topic(
            "test_stream",
            "test_topic",
            100,
            MessengerDuration::from_str("5ms").unwrap(),
        )
        .unwrap();
        let config = MessengerStreamConfig::new(consumer_config, producer_config);
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.consumer_config().batch_length(), 100);
        assert_eq!(config.producer_config().batch_length(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            MessengerDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().linger_time(),
            MessengerDuration::from_str("5ms").unwrap()
        );
    }

    #[test]
    fn should_be_default() {
        let config = MessengerStreamConfig::default();
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.consumer_config().batch_length(), 100);
        assert_eq!(config.producer_config().batch_length(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            MessengerDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().linger_time(),
            MessengerDuration::from_str("5ms").unwrap()
        );
    }

    #[test]
    fn should_be_from_stream_topic() {
        let res = MessengerStreamConfig::from_stream_topic(
            "test_stream",
            "test_topic",
            100,
            MessengerDuration::from_str("5ms").unwrap(),
            MessengerDuration::from_str("5ms").unwrap(),
        );

        assert!(res.is_ok());
        let config = res.unwrap();

        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.consumer_config().batch_length(), 100);
        assert_eq!(config.producer_config().batch_length(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            MessengerDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().linger_time(),
            MessengerDuration::from_str("5ms").unwrap()
        );
    }
}
