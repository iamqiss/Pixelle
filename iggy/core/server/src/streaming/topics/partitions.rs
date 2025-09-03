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

use crate::streaming::partitions::partition::Partition;
use crate::streaming::topics::COMPONENT;
use crate::streaming::topics::topic::Topic;
use error_set::ErrContext;
use iggy_common::locking::{IggySharedMut, IggySharedMutFn};
use iggy_common::{IggyError, IggyTimestamp};

const MAX_PARTITIONS_COUNT: u32 = 100_000;

impl Topic {
    pub fn has_partitions(&self) -> bool {
        !self.partitions.is_empty()
    }

    pub fn get_partitions_count(&self) -> u32 {
        self.partitions.len() as u32
    }

    pub async fn add_partitions(&mut self, count: u32) -> Result<Vec<u32>, IggyError> {
        if count == 0 {
            return Ok(vec![]);
        }

        let current_partitions_count = self.partitions.len() as u32;
        if current_partitions_count + count > MAX_PARTITIONS_COUNT {
            return Err(IggyError::TooManyPartitions);
        }

        let mut partition_ids = Vec::with_capacity(count as usize);
        for partition_id in current_partitions_count + 1..=current_partitions_count + count {
            let partition = Partition::create(
                self.stream_id,
                self.topic_id,
                partition_id,
                true,
                self.config.clone(),
                self.storage.clone(),
                self.message_expiry,
                self.messages_count_of_parent_stream.clone(),
                self.messages_count.clone(),
                self.size_of_parent_stream.clone(),
                self.size_bytes.clone(),
                self.segments_count_of_parent_stream.clone(),
                IggyTimestamp::now(),
            )
            .await;
            self.partitions
                .insert(partition_id, IggySharedMut::new(partition));
            partition_ids.push(partition_id)
        }

        Ok(partition_ids)
    }

    pub async fn add_persisted_partitions(&mut self, count: u32) -> Result<Vec<u32>, IggyError> {
        let partition_ids = self
            .add_partitions(count)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to add partitions, count: {count}")
            })?;
        for partition_id in &partition_ids {
            let partition = self.partitions.get(partition_id).unwrap();
            let mut partition = partition.write().await;
            partition.persist().await.with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to persist partition with id: {}",
                    partition.partition_id
                )
            })?;
        }
        Ok(partition_ids)
    }

    pub async fn delete_persisted_partitions(
        &mut self,
        mut count: u32,
    ) -> Result<Option<DeletedPartitions>, IggyError> {
        if count == 0 {
            return Ok(None);
        }

        let current_partitions_count = self.partitions.len() as u32;
        if count > current_partitions_count {
            count = current_partitions_count;
        }

        let mut segments_count = 0;
        let mut messages_count = 0;
        for partition_id in current_partitions_count - count + 1..=current_partitions_count {
            let partition = self.partitions.remove(&partition_id).unwrap();
            let mut partition = partition.write().await;
            let partition_messages_count = partition.get_messages_count();
            segments_count += partition.get_segments_count();
            messages_count += partition_messages_count;
            partition.delete().await.with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to delete partition with ID: {partition_id} in topic with ID: {}",
                    self.topic_id
                )
            })?;
        }
        Ok(Some(DeletedPartitions {
            segments_count,
            messages_count,
        }))
    }
}

pub struct DeletedPartitions {
    pub segments_count: u32,
    pub messages_count: u64,
}
