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
use messenger_binary_protocol::UserClient;
use messenger_common::locking::MessengerSharedMutFn;
use messenger_common::{
    Identifier, IdentityInfo, MessengerError, Permissions, UserInfo, UserInfoDetails, UserStatus,
};

#[async_trait]
impl UserClient for MessengerClient {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, MessengerError> {
        self.client.read().await.get_user(user_id).await
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, MessengerError> {
        self.client.read().await.get_users().await
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, MessengerError> {
        self.client
            .read()
            .await
            .create_user(username, password, status, permissions)
            .await
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), MessengerError> {
        self.client.read().await.delete_user(user_id).await
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), MessengerError> {
        self.client
            .read()
            .await
            .update_user(user_id, username, status)
            .await
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), MessengerError> {
        self.client
            .read()
            .await
            .update_permissions(user_id, permissions)
            .await
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), MessengerError> {
        self.client
            .read()
            .await
            .change_password(user_id, current_password, new_password)
            .await
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, MessengerError> {
        self.client
            .read()
            .await
            .login_user(username, password)
            .await
    }

    async fn logout_user(&self) -> Result<(), MessengerError> {
        self.client.read().await.logout_user().await
    }
}
