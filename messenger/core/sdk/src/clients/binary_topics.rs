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

use crate::prelude::MessengerClient;
use async_trait::async_trait;
use messenger_binary_protocol::TopicClient;
use messenger_common::locking::MessengerSharedMutFn;
use messenger_common::{
    CompressionAlgorithm, Identifier, MessengerError, MessengerExpiry, MaxTopicSize, Topic, TopicDetails,
};

#[async_trait]
impl TopicClient for MessengerClient {
    async fn get_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Option<TopicDetails>, MessengerError> {
        self.client
            .read()
            .await
            .get_topic(stream_id, topic_id)
            .await
    }

    async fn get_topics(&self, stream_id: &Identifier) -> Result<Vec<Topic>, MessengerError> {
        self.client.read().await.get_topics(stream_id).await
    }

    async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: &str,
        partitions_count: u32,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        topic_id: Option<u32>,
        message_expiry: MessengerExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<TopicDetails, MessengerError> {
        self.client
            .read()
            .await
            .create_topic(
                stream_id,
                name,
                partitions_count,
                compression_algorithm,
                replication_factor,
                topic_id,
                message_expiry,
                max_topic_size,
            )
            .await
    }

    async fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        compression_algorithm: CompressionAlgorithm,
        replication_factor: Option<u8>,
        message_expiry: MessengerExpiry,
        max_topic_size: MaxTopicSize,
    ) -> Result<(), MessengerError> {
        self.client
            .read()
            .await
            .update_topic(
                stream_id,
                topic_id,
                name,
                compression_algorithm,
                replication_factor,
                message_expiry,
                max_topic_size,
            )
            .await
    }

    async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), MessengerError> {
        self.client
            .read()
            .await
            .delete_topic(stream_id, topic_id)
            .await
    }

    async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), MessengerError> {
        self.client
            .read()
            .await
            .purge_topic(stream_id, topic_id)
            .await
    }
}
