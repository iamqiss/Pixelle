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

use crate::utils::auth::fail_if_not_authenticated;
use crate::utils::mapper;
use crate::{BinaryClient, StreamClient};
use messenger_common::create_stream::CreateStream;
use messenger_common::delete_stream::DeleteStream;
use messenger_common::get_stream::GetStream;
use messenger_common::get_streams::GetStreams;
use messenger_common::purge_stream::PurgeStream;
use messenger_common::update_stream::UpdateStream;
use messenger_common::{Identifier, MessengerError, Stream, StreamDetails};

#[async_trait::async_trait]
impl<B: BinaryClient> StreamClient for B {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, MessengerError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&GetStream {
                stream_id: stream_id.clone(),
            })
            .await?;
        if response.is_empty() {
            return Ok(None);
        }

        mapper::map_stream(response).map(Some)
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, MessengerError> {
        fail_if_not_authenticated(self).await?;
        let response = self.send_with_response(&GetStreams {}).await?;
        mapper::map_streams(response)
    }

    async fn create_stream(
        &self,
        name: &str,
        stream_id: Option<u32>,
    ) -> Result<StreamDetails, MessengerError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&CreateStream {
                name: name.to_string(),
                stream_id,
            })
            .await?;
        mapper::map_stream(response)
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), MessengerError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&UpdateStream {
            stream_id: stream_id.clone(),
            name: name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), MessengerError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeleteStream {
            stream_id: stream_id.clone(),
        })
        .await?;
        Ok(())
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), MessengerError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&PurgeStream {
            stream_id: stream_id.clone(),
        })
        .await?;
        Ok(())
    }
}
