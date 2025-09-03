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

use crate::binary::command::{BinaryServerCommand, ServerCommandHandler};
use crate::binary::sender::SenderKind;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use anyhow::Result;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::locking::IggySharedMutFn;
use iggy_common::ping::Ping;
use tracing::debug;

impl ServerCommandHandler for Ping {
    fn code(&self) -> u32 {
        iggy_common::PING_CODE
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
        let client_manager = system.client_manager.read().await;
        if let Some(client) = client_manager.try_get_client(session.client_id) {
            let mut client = client.write().await;
            let now = IggyTimestamp::now();
            client.last_heartbeat = now;
            debug!("Updated last heartbeat to: {now} for session: {session}");
        }

        sender.send_empty_ok_response().await?;
        Ok(())
    }
}

impl BinaryServerCommand for Ping {
    async fn from_sender(
        _sender: &mut SenderKind,
        _length: u32,
        _code: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        Ok(Ping {})
    }
}
