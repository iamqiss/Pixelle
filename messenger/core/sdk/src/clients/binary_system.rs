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
use messenger_binary_protocol::SystemClient;
use messenger_common::locking::MessengerSharedMutFn;
use messenger_common::{
    ClientInfo, ClientInfoDetails, MessengerDuration, MessengerError, Snapshot, SnapshotCompression, Stats,
    SystemSnapshotType,
};

#[async_trait]
impl SystemClient for MessengerClient {
    async fn get_stats(&self) -> Result<Stats, MessengerError> {
        self.client.read().await.get_stats().await
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, MessengerError> {
        self.client.read().await.get_me().await
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, MessengerError> {
        self.client.read().await.get_client(client_id).await
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, MessengerError> {
        self.client.read().await.get_clients().await
    }

    async fn ping(&self) -> Result<(), MessengerError> {
        self.client.read().await.ping().await
    }

    async fn heartbeat_interval(&self) -> MessengerDuration {
        self.client.read().await.heartbeat_interval().await
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, MessengerError> {
        self.client
            .read()
            .await
            .snapshot(compression, snapshot_types)
            .await
    }
}
