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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::prelude::Identifier;
use crate::prelude::MessengerError;
use async_trait::async_trait;
use messenger_binary_protocol::StreamClient;
use messenger_common::create_stream::CreateStream;
use messenger_common::update_stream::UpdateStream;
use messenger_common::{Stream, StreamDetails};

const PATH: &str = "/streams";

#[async_trait]
impl StreamClient for HttpClient {
    async fn get_stream(&self, stream_id: &Identifier) -> Result<Option<StreamDetails>, MessengerError> {
        let response = self.get(&get_details_path(&stream_id.as_cow_str())).await;
        if let Err(error) = response {
            if matches!(error, MessengerError::ResourceNotFound(_)) {
                return Ok(None);
            }

            return Err(error);
        }

        let stream = response?
            .json()
            .await
            .map_err(|_| MessengerError::InvalidJsonResponse)?;
        Ok(Some(stream))
    }

    async fn get_streams(&self) -> Result<Vec<Stream>, MessengerError> {
        let response = self.get(PATH).await?;
        let streams = response
            .json()
            .await
            .map_err(|_| MessengerError::InvalidJsonResponse)?;
        Ok(streams)
    }

    async fn create_stream(
        &self,
        name: &str,
        stream_id: Option<u32>,
    ) -> Result<StreamDetails, MessengerError> {
        let response = self
            .post(
                PATH,
                &CreateStream {
                    name: name.to_string(),
                    stream_id,
                },
            )
            .await?;
        let stream = response
            .json()
            .await
            .map_err(|_| MessengerError::InvalidJsonResponse)?;
        Ok(stream)
    }

    async fn update_stream(&self, stream_id: &Identifier, name: &str) -> Result<(), MessengerError> {
        self.put(
            &get_details_path(&stream_id.as_cow_str()),
            &UpdateStream {
                stream_id: stream_id.clone(),
                name: name.to_string(),
            },
        )
        .await?;
        Ok(())
    }

    async fn delete_stream(&self, stream_id: &Identifier) -> Result<(), MessengerError> {
        self.delete(&get_details_path(&stream_id.as_cow_str()))
            .await?;
        Ok(())
    }

    async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), MessengerError> {
        self.delete(&format!(
            "{}/purge",
            get_details_path(&stream_id.as_cow_str())
        ))
        .await?;
        Ok(())
    }
}

fn get_details_path(stream_id: &str) -> String {
    format!("{PATH}/{stream_id}")
}
