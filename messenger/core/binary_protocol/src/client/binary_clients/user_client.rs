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

use async_trait::async_trait;
use messenger_common::{
    Identifier, IdentityInfo, MessengerError, Permissions, UserInfo, UserInfoDetails, UserStatus,
};

/// This trait defines the methods to interact with the user module.
#[async_trait]
pub trait UserClient {
    /// Get the info about a specific user by unique ID or username.
    ///
    /// Authentication is required, and the permission to read the users, unless the provided user ID is the same as the authenticated user.
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, MessengerError>;
    /// Get the info about all the users.
    ///
    /// Authentication is required, and the permission to read the users.
    async fn get_users(&self) -> Result<Vec<UserInfo>, MessengerError>;
    /// Create a new user.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
    ) -> Result<UserInfoDetails, MessengerError>;
    /// Delete a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn delete_user(&self, user_id: &Identifier) -> Result<(), MessengerError>;
    /// Update a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn update_user(
        &self,
        user_id: &Identifier,
        username: Option<&str>,
        status: Option<UserStatus>,
    ) -> Result<(), MessengerError>;
    /// Update the permissions of a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users.
    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), MessengerError>;
    /// Change the password of a user by unique ID or username.
    ///
    /// Authentication is required, and the permission to manage the users, unless the provided user ID is the same as the authenticated user.
    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), MessengerError>;
    /// Login a user by username and password.
    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, MessengerError>;
    /// Logout the currently authenticated user.
    async fn logout_user(&self) -> Result<(), MessengerError>;
}
