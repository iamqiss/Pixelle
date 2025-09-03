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
use messenger::prelude::PersonalAccessTokenExpiry;
use predicates::str::diff;
use serial_test::parallel;

struct TestPatDeleteCmd {
    name: String,
}

impl TestPatDeleteCmd {
    fn new(name: String) -> Self {
        Self { name }
    }

    fn to_args(&self) -> Vec<String> {
        vec![self.name.clone()]
    }
}

#[async_trait]
impl MessengerCmdTestCase for TestPatDeleteCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let pat = client
            .create_personal_access_token(&self.name, PersonalAccessTokenExpiry::NeverExpire)
            .await;
        assert!(pat.is_ok());
    }

    fn get_command(&self) -> MessengerCmdCommand {
        MessengerCmdCommand::new()
            .arg("pat")
            .arg("delete")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let message = format!(
            "Executing delete personal access tokens with name: {}\nPersonal access token with name: {} deleted\n",
            self.name, self.name
        );

        command_state.success().stdout(diff(message));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let tokens = client.get_personal_access_tokens().await;

        assert!(tokens.is_ok());
        let tokens = tokens.unwrap();
        assert!(tokens.is_empty());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test.setup().await;
    messenger_cmd_test
        .execute_test(TestPatDeleteCmd::new(String::from("name")))
        .await;
    messenger_cmd_test
        .execute_test(TestPatDeleteCmd::new(String::from("client")))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut messenger_cmd_test = MessengerCmdTest::help_message();

    messenger_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["pat", "delete", "--help"],
            format!(
                r#"Delete personal access token

Examples
 messenger pat delete name
 messenger pat delete client

{USAGE_PREFIX} pat delete <NAME>

Arguments:
  <NAME>
          Personal access token name to delete

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
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["pat", "delete", "-h"],
            format!(
                r#"Delete personal access token

{USAGE_PREFIX} pat delete <NAME>

Arguments:
  <NAME>  Personal access token name to delete

Options:
  -h, --help  Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
