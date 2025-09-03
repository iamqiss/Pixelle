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

use crate::cli::common::{MessengerCmdCommand, MessengerCmdTest, MessengerCmdTestCase, TestHelpCmd, USAGE_PREFIX};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use messenger::prelude::Client;
use integration::test_server::TestServer;
use predicates::str::{contains, diff, starts_with};
use serial_test::parallel;
use std::fmt::Display;

#[derive(Debug, Default)]
pub(super) enum Protocol {
    #[default]
    Tcp,
    Quic,
}

#[derive(Debug, Default)]
pub(super) enum Scenario {
    #[default]
    SuccessWithCredentials,
    SuccessWithoutCredentials,
    FailureWithoutCredentials,
    FailureDueToSessionTimeout(String),
}

impl Protocol {
    fn as_arg(&self) -> Vec<&str> {
        match self {
            Self::Tcp => vec!["--transport", "tcp"],
            Self::Quic => vec!["--transport", "quic"],
        }
    }
}

impl Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp => write!(f, "TCP"),
            Self::Quic => write!(f, "QUIC"),
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct TestMeCmd {
    protocol: Protocol,
    scenario: Scenario,
}

impl TestMeCmd {
    pub(super) fn new(protocol: Protocol, scenario: Scenario) -> Self {
        Self { protocol, scenario }
    }
}

#[async_trait]
impl MessengerCmdTestCase for TestMeCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> MessengerCmdCommand {
        let command = MessengerCmdCommand::new().opts(self.protocol.as_arg()).arg("me");

        match &self.scenario {
            Scenario::SuccessWithCredentials => command.with_env_credentials(),
            Scenario::FailureWithoutCredentials => command.disable_backtrace(),
            Scenario::FailureDueToSessionTimeout(_) => command.disable_backtrace(),
            _ => command,
        }
    }

    fn verify_command(&self, command_state: Assert) {
        match &self.scenario {
            Scenario::SuccessWithCredentials | Scenario::SuccessWithoutCredentials => {
                command_state
                    .success()
                    .stdout(starts_with("Executing me command\n"))
                    .stdout(contains(format!("Transport | {}", self.protocol)));
            }
            Scenario::FailureWithoutCredentials => {
                command_state
                    .failure()
                    .stderr(diff("Error: CommandError(Messenger command line tool error\n\nCaused by:\n    Missing messenger server credentials)\n"));
            }
            Scenario::FailureDueToSessionTimeout(server_address) => {
                command_state.failure().stderr(diff(format!("Error: CommandError(Login session expired for Messenger server: {server_address}, please login again or use other authentication method)\n")));
            }
        }
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}

    fn protocol(&self, server: &TestServer) -> Vec<String> {
        match &self.protocol {
            Protocol::Tcp => vec![
                "--tcp-server-address".into(),
                server.get_raw_tcp_addr().unwrap(),
            ],
            Protocol::Quic => vec![
                "--quic-server-address".into(),
                server.get_quic_udp_addr().unwrap(),
            ],
        }
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test.setup().await;
    messenger_cmd_test.execute_test(TestMeCmd::default()).await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful_using_transport_tcp() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test.setup().await;
    messenger_cmd_test
        .execute_test(TestMeCmd::new(
            Protocol::Tcp,
            Scenario::SuccessWithCredentials,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful_using_transport_quic() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test.setup().await;
    messenger_cmd_test
        .execute_test(TestMeCmd::new(
            Protocol::Quic,
            Scenario::SuccessWithCredentials,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_be_unsuccessful_using_transport_tcp() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test.setup().await;
    messenger_cmd_test
        .execute_test(TestMeCmd::new(
            Protocol::Tcp,
            Scenario::FailureWithoutCredentials,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut messenger_cmd_test = MessengerCmdTest::help_message();

    messenger_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["me", "--help"],
            format!(
                r#"get current client info

Command connects to Messenger server and collects client info like client ID, user ID
server address and protocol type.

{USAGE_PREFIX} me

Options:
  -h, --help
          Print help (see a summary with '-h')
"#,
            ),
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_short_help_match() {
    let mut messenger_cmd_test = MessengerCmdTest::help_message();

    messenger_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["me", "-h"],
            format!(
                r#"get current client info

{USAGE_PREFIX} me

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
