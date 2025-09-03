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

use crate::cli::common::{
    CLAP_INDENT, MessengerCmdCommand, MessengerCmdTest, MessengerCmdTestCase, TestHelpCmd, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use messenger::prelude::Client;
use messenger::prelude::Identifier;
use messenger::prelude::MessengerExpiry;
use messenger::prelude::MaxTopicSize;
use messenger_binary_protocol::cli::binary_system::stats::GetStatsOutput;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TestStatsCmdOutput {
    Default,
    Set(GetStatsOutput),
}

struct TestStatsCmd {
    test_output: TestStatsCmdOutput,
}

impl TestStatsCmd {
    fn new(test_output: TestStatsCmdOutput) -> Self {
        Self { test_output }
    }
    fn get_cmd(&self) -> MessengerCmdCommand {
        let command = MessengerCmdCommand::new().arg("stats").with_env_credentials();

        match self.test_output {
            TestStatsCmdOutput::Set(option) => {
                let command = command.arg("-o").arg(format!("{option}"));
                if option != GetStatsOutput::Table {
                    command.opt("-q")
                } else {
                    command
                }
            }
            _ => command,
        }
    }
}

#[async_trait]
impl MessengerCmdTestCase for TestStatsCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream_id = Identifier::from_str_value("logs").unwrap();
        let stream = client.create_stream(&stream_id.as_string(), Some(1)).await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &stream_id,
                "topic",
                5,
                Default::default(),
                None,
                Some(1),
                MessengerExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> MessengerCmdCommand {
        self.get_cmd()
    }

    fn verify_command(&self, command_state: Assert) {
        match self.test_output {
            TestStatsCmdOutput::Default | TestStatsCmdOutput::Set(GetStatsOutput::Table) => {
                command_state
                    .success()
                    .stdout(starts_with("Executing stats command\n"))
                    .stdout(contains("Streams Count            | 1"))
                    .stdout(contains("Topics Count             | 1"))
                    .stdout(contains("Partitions Count         | 5"))
                    .stdout(contains("Segments Count           | 5"))
                    .stdout(contains("Message Count            | 0"))
                    .stdout(contains("Clients Count            | 2")) // 2 clients are connected during test
                    .stdout(contains("Consumer Groups Count    | 0"));
            }
            TestStatsCmdOutput::Set(GetStatsOutput::List) => {
                command_state
                    .success()
                    .stdout(contains("Streams Count|1"))
                    .stdout(contains("Topics Count|1"))
                    .stdout(contains("Partitions Count|5"))
                    .stdout(contains("Segments Count|5"))
                    .stdout(contains("Message Count|0"))
                    .stdout(contains("Clients Count|2")) // 2 clients are connected during test
                    .stdout(contains("Consumer Groups Count|0"));
            }
            TestStatsCmdOutput::Set(GetStatsOutput::Json) => {
                command_state
                    .success()
                    .stdout(contains(r#""streams_count": 1"#))
                    .stdout(contains(r#""topics_count": 1"#))
                    .stdout(contains(r#""partitions_count": 5"#))
                    .stdout(contains(r#""segments_count": 5"#))
                    .stdout(contains(r#""messages_count": 0"#))
                    .stdout(contains(r#""clients_count": 2"#)) // 2 clients are connected during test
                    .stdout(contains(r#""consumer_groups_count": 0"#));
            }
            TestStatsCmdOutput::Set(GetStatsOutput::Toml) => {
                command_state
                    .success()
                    .stdout(contains("streams_count = 1"))
                    .stdout(contains("topics_count = 1"))
                    .stdout(contains("partitions_count = 5"))
                    .stdout(contains("segments_count = 5"))
                    .stdout(contains("messages_count = 0"))
                    .stdout(contains("clients_count = 2")) // 2 clients are connected during test
                    .stdout(contains("consumer_groups_count = 0"));
            }
        }
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .delete_topic(&1.try_into().unwrap(), &1.try_into().unwrap())
            .await;
        assert!(topic.is_ok());

        let stream = client.delete_stream(&1.try_into().unwrap()).await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test.setup().await;
    messenger_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Default))
        .await;
    messenger_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Set(
            GetStatsOutput::Table,
        )))
        .await;
    messenger_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Set(
            GetStatsOutput::List,
        )))
        .await;
    messenger_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Set(
            GetStatsOutput::Json,
        )))
        .await;
    messenger_cmd_test
        .execute_test(TestStatsCmd::new(TestStatsCmdOutput::Set(
            GetStatsOutput::Toml,
        )))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut messenger_cmd_test = MessengerCmdTest::help_message();

    messenger_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["stats", "--help"],
            format!(
                r#"get messenger server statistics

Collect basic Messenger server statistics like number of streams, topics, partitions, etc.
Server OS name, version, etc. are also collected.

{USAGE_PREFIX} stats [OPTIONS]

Options:
  -o, --output <OUTPUT>
          List mode (table, list, JSON, TOML)
{CLAP_INDENT}
          [default: table]
          [possible values: table, list, json, toml]

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
            vec!["stats", "-h"],
            format!(
                r#"get messenger server statistics

{USAGE_PREFIX} stats [OPTIONS]

Options:
  -o, --output <OUTPUT>  List mode (table, list, JSON, TOML) [default: table] [possible values: table, list, json, toml]
  -h, --help             Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
