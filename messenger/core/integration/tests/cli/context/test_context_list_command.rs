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

use std::collections::HashMap;

use crate::cli::common::{
    CLAP_INDENT, MessengerCmdCommand, MessengerCmdTest, MessengerCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use messenger::prelude::Client;
use messenger_binary_protocol::cli::binary_context::common::ContextConfig;
use predicates::str::contains;
use serial_test::parallel;

use super::common::TestMessengerContext;

struct TestContextListCmd {
    test_messenger_context: TestMessengerContext,
}

impl TestContextListCmd {
    fn new(test_messenger_context: TestMessengerContext) -> Self {
        Self { test_messenger_context }
    }
}

#[async_trait]
impl MessengerCmdTestCase for TestContextListCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {
        self.test_messenger_context.prepare().await;
    }

    fn get_command(&self) -> MessengerCmdCommand {
        MessengerCmdCommand::new()
            .env(
                "MESSENGER_HOME",
                self.test_messenger_context.get_messenger_home().to_str().unwrap(),
            )
            .arg("context")
            .arg("list")
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let maybe_context_map = self.test_messenger_context.get_contexts();
        let active_context_key = self.test_messenger_context.get_active_context_key();

        let mut command_state = command_state.success();

        if let Some(context_map) = maybe_context_map {
            for (key, _) in context_map {
                if let Some(active_context_key) = &active_context_key {
                    // The active context should have an asterisk (*) by its name
                    if key.eq(active_context_key) {
                        command_state = command_state.stdout(contains(format!("{key}*")));
                    }
                } else {
                    // if there's no active context key (i.e. no file for it)
                    // then the default context should have an asterisk (*) by its name
                    if key.eq("default") {
                        command_state = command_state.stdout(contains(format!("{key}*")));
                    } else {
                        command_state = command_state.stdout(contains(key));
                    }
                }
            }
        } else {
            // if there's no context map (i.e. no file for it)
            // there should still be a default context with an asterisk (*) by its name
            command_state.stdout(contains("default*"));
        }
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut messenger_cmd_test = MessengerCmdTest::default();
    messenger_cmd_test.setup().await;

    messenger_cmd_test
        .execute_test(TestContextListCmd::new(TestMessengerContext::new(
            Some(HashMap::from([
                ("default".to_string(), ContextConfig::default()),
                ("second".to_string(), ContextConfig::default()),
            ])),
            None,
        )))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_display_active_context() {
    let mut messenger_cmd_test = MessengerCmdTest::default();
    messenger_cmd_test.setup().await;

    messenger_cmd_test
        .execute_test(TestContextListCmd::new(TestMessengerContext::new(
            Some(HashMap::from([
                (
                    "default".to_string(),
                    ContextConfig {
                        ..Default::default()
                    },
                ),
                (
                    "second".to_string(),
                    ContextConfig {
                        ..Default::default()
                    },
                ),
            ])),
            Some("second".to_string()),
        )))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_display_default() {
    let mut messenger_cmd_test = MessengerCmdTest::default();
    messenger_cmd_test.setup().await;

    messenger_cmd_test
        .execute_test(TestContextListCmd::new(TestMessengerContext::new(None, None)))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["context", "list", "--help"],
            format!(
                r#"List all contexts

Examples
 messenger context list

{USAGE_PREFIX} context list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>
          List mode (table or list)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list]

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
            vec!["context", "list", "-h"],
            format!(
                r#"List all contexts

{USAGE_PREFIX} context list [OPTIONS]

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
