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

use crate::{BinaryClient, ConsumerGroupClient};

use crate::utils::auth::fail_if_not_authenticated;
use crate::utils::mapper;
use messenger_common::create_consumer_group::CreateConsumerGroup;
use messenger_common::delete_consumer_group::DeleteConsumerGroup;
use messenger_common::get_consumer_group::GetConsumerGroup;
use messenger_common::get_consumer_groups::GetConsumerGroups;
use messenger_common::join_consumer_group::JoinConsumerGroup;
use messenger_common::leave_consumer_group::LeaveConsumerGroup;
use messenger_common::{ConsumerGroup, ConsumerGroupDetails, Identifier, MessengerError};

#[async_trait::async_trait]
impl<B: BinaryClient> ConsumerGroupClient for B {
    async fn get_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<Option<ConsumerGroupDetails>, MessengerError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                group_id: group_id.clone(),
            })
            .await?;
        if response.is_empty() {
            return Ok(None);
        }

        mapper::map_consumer_group(response).map(Some)
    }

    async fn get_consumer_groups(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<Vec<ConsumerGroup>, MessengerError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetConsumerGroups {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
            })
            .await?;
        mapper::map_consumer_groups(response)
    }

    async fn create_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: &str,
        group_id: Option<u32>,
    ) -> Result<ConsumerGroupDetails, MessengerError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&CreateConsumerGroup {
                stream_id: stream_id.clone(),
                topic_id: topic_id.clone(),
                name: name.to_string(),
                group_id,
            })
            .await?;
        mapper::map_consumer_group(response)
    }

    async fn delete_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), MessengerError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeleteConsumerGroup {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            group_id: group_id.clone(),
        })
        .await?;
        Ok(())
    }

    async fn join_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), MessengerError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&JoinConsumerGroup {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            group_id: group_id.clone(),
        })
        .await?;
        Ok(())
    }

    async fn leave_consumer_group(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), MessengerError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&LeaveConsumerGroup {
            stream_id: stream_id.clone(),
            topic_id: topic_id.clone(),
            group_id: group_id.clone(),
        })
        .await?;
        Ok(())
    }
}
