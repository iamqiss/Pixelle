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

use crate::args::CliOptions;
use crate::error::{CmdToolError, MessengerCmdError};
use anyhow::{Context, bail};
use messenger::clients::client::MessengerClient;
use messenger::prelude::{Args, MessengerError, PersonalAccessTokenClient, UserClient};
use messenger_binary_protocol::cli::binary_system::session::ServerSession;
use passterm::{Stream, isatty, prompt_password_stdin, prompt_password_tty};
use std::env::var;

#[cfg(feature = "login-session")]
mod credentials_login_session {
    pub(crate) use messenger_binary_protocol::cli::cli_command::PRINT_TARGET;
    pub(crate) use keyring::Entry;
    pub(crate) use tracing::{Level, event};
}

#[cfg(feature = "login-session")]
use credentials_login_session::*;

static ENV_MESSENGER_USERNAME: &str = "MESSENGER_USERNAME";
static ENV_MESSENGER_PASSWORD: &str = "MESSENGER_PASSWORD";

struct MessengerUserClient {
    username: String,
    password: String,
}

enum Credentials {
    UserNameAndPassword(MessengerUserClient),
    PersonalAccessToken(String),
    SessionWithToken(String, String),
}

pub(crate) struct MessengerCredentials<'a> {
    credentials: Option<Credentials>,
    messenger_client: Option<&'a MessengerClient>,
    login_required: bool,
}

impl<'a> MessengerCredentials<'a> {
    pub(crate) fn new(
        cli_options: &CliOptions,
        messenger_args: &Args,
        login_required: bool,
    ) -> anyhow::Result<Self, anyhow::Error> {
        if !login_required {
            return Ok(Self {
                credentials: None,
                messenger_client: None,
                login_required,
            });
        }

        if let Some(server_address) = messenger_args.get_server_address() {
            let server_session = ServerSession::new(server_address.clone());
            if let Some(token) = server_session.get_token() {
                return Ok(Self {
                    credentials: Some(Credentials::SessionWithToken(token, server_address)),
                    messenger_client: None,
                    login_required,
                });
            }
        }

        #[cfg(feature = "login-session")]
        if let Some(token_name) = &cli_options.token_name {
            return match messenger_args.get_server_address() {
                Some(server_address) => {
                    let server_address = format!("messenger:{server_address}");
                    event!(target: PRINT_TARGET, Level::DEBUG,"Checking token presence under service: {} and name: {}",
                    server_address, token_name);
                    let entry = Entry::new(&server_address, token_name)?;
                    let token = entry.get_password()?;

                    Ok(Self {
                        credentials: Some(Credentials::PersonalAccessToken(token)),
                        messenger_client: None,
                        login_required,
                    })
                }
                None => Err(MessengerCmdError::CmdToolError(CmdToolError::MissingServerAddress).into()),
            };
        }

        if let Some(token) = &cli_options.token {
            Ok(Self {
                credentials: Some(Credentials::PersonalAccessToken(token.clone())),
                messenger_client: None,
                login_required,
            })
        } else if let Some(username) = &cli_options.username {
            let password = match &cli_options.password {
                Some(password) => password.clone(),
                None => {
                    if isatty(Stream::Stdin) {
                        prompt_password_tty(Some("Password: "))?
                    } else {
                        prompt_password_stdin(None, Stream::Stdout)?
                    }
                }
            };

            Ok(Self {
                credentials: Some(Credentials::UserNameAndPassword(MessengerUserClient {
                    username: username.clone(),
                    password,
                })),
                messenger_client: None,
                login_required,
            })
        } else if var(ENV_MESSENGER_USERNAME).is_ok() && var(ENV_MESSENGER_PASSWORD).is_ok() {
            Ok(Self {
                credentials: Some(Credentials::UserNameAndPassword(MessengerUserClient {
                    username: var(ENV_MESSENGER_USERNAME)?,
                    password: var(ENV_MESSENGER_PASSWORD)?,
                })),
                messenger_client: None,
                login_required,
            })
        } else {
            Err(MessengerCmdError::CmdToolError(CmdToolError::MissingCredentials).into())
        }
    }

    pub(crate) fn set_messenger_client(&mut self, messenger_client: &'a MessengerClient) {
        self.messenger_client = Some(messenger_client);
    }

    pub(crate) async fn login_user(&self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(client) = self.messenger_client
            && self.login_required
        {
            let credentials = self.credentials.as_ref().unwrap();
            match credentials {
                Credentials::UserNameAndPassword(username_and_password) => {
                    let _ = client
                        .login_user(
                            &username_and_password.username,
                            &username_and_password.password,
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "Problem with server login for username: {}",
                                &username_and_password.username
                            )
                        })?;
                }
                Credentials::PersonalAccessToken(token_value) => {
                    let _ = client
                        .login_with_personal_access_token(token_value)
                        .await
                        .with_context(|| {
                            format!("Problem with server login with token: {token_value}")
                        })?;
                }
                Credentials::SessionWithToken(token_value, server_address) => {
                    let login_result = client.login_with_personal_access_token(token_value).await;
                    if let Err(err) = login_result {
                        if matches!(
                            err,
                            MessengerError::Unauthenticated
                                | MessengerError::ResourceNotFound(_)
                                | MessengerError::PersonalAccessTokenExpired(_, _)
                        ) {
                            let server_session = ServerSession::new(server_address.clone());
                            server_session.delete()?;
                            bail!(
                                "Login session expired for Messenger server: {server_address}, please login again or use other authentication method"
                            );
                        } else {
                            bail!("Problem with server login with token: {token_value}");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) async fn logout_user(&self) -> anyhow::Result<(), anyhow::Error> {
        if let Some(client) = self.messenger_client
            && self.login_required
        {
            client
                .logout_user()
                .await
                .with_context(|| "Problem with server logout".to_string())?;
        }

        Ok(())
    }
}
