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

use crate::binary::command::{BinaryServerCommand, ServerCommand, ServerCommandHandler};
use crate::binary::handlers::consumer_offsets::COMPONENT;
use crate::binary::handlers::utils::receive_and_validate;
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use error_set::ErrContext;
use iggy_common::IggyError;
use iggy_common::delete_consumer_offset::DeleteConsumerOffset;
use tracing::debug;

impl ServerCommandHandler for DeleteConsumerOffset {
    fn code(&self) -> u32 {
        iggy_common::DELETE_CONSUMER_OFFSET_CODE
    }

    async fn handle(
        self,
        sender: &mut SenderKind,
        _length: u32,
        session: &Session,
        system: &SharedSystem,
    ) -> Result<(), IggyError> {
        debug!("session: {session}, command: {self}");
        let system = system.read().await;
        system
            .delete_consumer_offset(
                session,
                self.consumer,
                &self.stream_id,
                &self.topic_id,
                self.partition_id,
            )
            .await
            .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to delete consumer offset for topic with ID: {} in stream with ID: {} partition ID: {:#?}, session: {}",
                self.topic_id, self.stream_id, self.partition_id, session
            ))?;
        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for DeleteConsumerOffset {
    async fn from_sender(sender: &mut SenderKind, code: u32, length: u32) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        match receive_and_validate(sender, code, length).await? {
            ServerCommand::DeleteConsumerOffset(delete_consumer_offset) => {
                Ok(delete_consumer_offset)
            }
            _ => Err(IggyError::InvalidCommand),
        }
    }
}
