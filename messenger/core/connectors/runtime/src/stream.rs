// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use messenger::prelude::{Client, MessengerClient, MessengerClientBuilder};
use tracing::{error, info};

use crate::{configs::MessengerConfig, error::RuntimeError};

pub struct MessengerClients {
    pub producer: MessengerClient,
    pub consumer: MessengerClient,
}

pub async fn init(config: MessengerConfig) -> Result<MessengerClients, RuntimeError> {
    let consumer = create_client(&config).await?;
    let producer = create_client(&config).await?;
    let messenger_clients = MessengerClients { producer, consumer };
    Ok(messenger_clients)
}

async fn create_client(config: &MessengerConfig) -> Result<MessengerClient, RuntimeError> {
    let address = config.address.to_owned();
    let username = config.username.to_owned();
    let password = config.password.to_owned();
    let token = config.token.to_owned();

    let connection_string = if let Some(token) = token {
        if token.is_empty() {
            error!("Messenger token cannot be empty (if username and password are not provided)");
            return Err(RuntimeError::MissingMessengerCredentials);
        }

        let redacted_token = token.chars().take(3).collect::<String>();
        info!("Using token: {redacted_token}*** for Messenger authentication");
        format!("messenger://{token}@{address}")
    } else {
        info!("Using username and password for Messenger authentication");
        let username = username.ok_or(RuntimeError::MissingMessengerCredentials)?;
        if username.is_empty() {
            error!("Messenger password cannot be empty (if token is not provided)");
            return Err(RuntimeError::MissingMessengerCredentials);
        }

        let password = password.ok_or(RuntimeError::MissingMessengerCredentials)?;
        if password.is_empty() {
            error!("Messenger password cannot be empty (if token is not provided)");
            return Err(RuntimeError::MissingMessengerCredentials);
        }

        let redacted_username = username.chars().take(3).collect::<String>();
        let redacted_password = password.chars().take(3).collect::<String>();
        info!(
            "Using username: {redacted_username}***, password: {redacted_password}*** for Messenger authentication"
        );
        format!("messenger://{username}:{password}@{address}")
    };

    let client = MessengerClientBuilder::from_connection_string(&connection_string)?.build()?;
    client.connect().await?;
    Ok(client)
}
