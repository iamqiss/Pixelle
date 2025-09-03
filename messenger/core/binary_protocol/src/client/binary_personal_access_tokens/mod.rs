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
use crate::{BinaryClient, PersonalAccessTokenClient};
use messenger_common::create_personal_access_token::CreatePersonalAccessToken;
use messenger_common::delete_personal_access_token::DeletePersonalAccessToken;
use messenger_common::get_personal_access_tokens::GetPersonalAccessTokens;
use messenger_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use messenger_common::{
    ClientState, IdentityInfo, MessengerError, PersonalAccessTokenExpiry, PersonalAccessTokenInfo,
    RawPersonalAccessToken,
};

#[async_trait::async_trait]
impl<B: BinaryClient> PersonalAccessTokenClient for B {
    async fn get_personal_access_tokens(&self) -> Result<Vec<PersonalAccessTokenInfo>, MessengerError> {
        fail_if_not_authenticated(self).await?;
        let response = self.send_with_response(&GetPersonalAccessTokens {}).await?;
        mapper::map_personal_access_tokens(response)
    }

    async fn create_personal_access_token(
        &self,
        name: &str,
        expiry: PersonalAccessTokenExpiry,
    ) -> Result<RawPersonalAccessToken, MessengerError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_with_response(&CreatePersonalAccessToken {
                name: name.to_string(),
                expiry,
            })
            .await?;
        mapper::map_raw_pat(response)
    }

    async fn delete_personal_access_token(&self, name: &str) -> Result<(), MessengerError> {
        fail_if_not_authenticated(self).await?;
        self.send_with_response(&DeletePersonalAccessToken {
            name: name.to_string(),
        })
        .await?;
        Ok(())
    }

    async fn login_with_personal_access_token(
        &self,
        token: &str,
    ) -> Result<IdentityInfo, MessengerError> {
        let response = self
            .send_with_response(&LoginWithPersonalAccessToken {
                token: token.to_string(),
            })
            .await?;
        self.set_state(ClientState::Authenticated).await;
        mapper::map_identity_info(response)
    }
}
