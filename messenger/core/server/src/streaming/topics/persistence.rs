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

use crate::state::system::TopicState;
use crate::streaming::topics::COMPONENT;
use crate::streaming::topics::topic::Topic;
use error_set::ErrContext;
use messenger_common::MessengerError;
use messenger_common::locking::MessengerSharedMutFn;

impl Topic {
    pub async fn load(&mut self, state: TopicState) -> Result<(), MessengerError> {
        let storage = self.storage.clone();
        storage.topic.load(self, state).await?;
        Ok(())
    }

    pub async fn persist(&self) -> Result<(), MessengerError> {
        self.storage.topic.save(self).await
    }

    pub async fn delete(&self) -> Result<(), MessengerError> {
        for partition in self.get_partitions() {
            let mut partition = partition.write().await;
            partition.delete().await.with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete partition with ID: {} in topic with ID: {}",
                    partition.partition_id, self.topic_id
                )
            })?;
        }

        self.storage.topic.delete(self).await
    }

    pub async fn persist_messages(&self) -> Result<usize, MessengerError> {
        let mut saved_messages_number = 0;
        for partition in self.get_partitions() {
            let mut partition = partition.write().await;
            let partition_id = partition.partition_id;
            for segment in partition.get_segments_mut() {
                saved_messages_number += segment.persist_messages(None).await.with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to persist messages in segment, partition ID: {partition_id}"))?;
            }
        }

        Ok(saved_messages_number)
    }

    pub async fn purge(&self) -> Result<(), MessengerError> {
        for partition in self.get_partitions() {
            let mut partition = partition.write().await;
            partition.purge().await?;
        }
        Ok(())
    }
}
