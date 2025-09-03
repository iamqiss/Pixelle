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
    CLAP_INDENT, MessengerCmdCommand, MessengerCmdTest, MessengerCmdTestCase, OutputFormat, TestHelpCmd,
    TestStreamId, USAGE_PREFIX,
};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use messenger::prelude::Client;
use messenger::prelude::MessengerExpiry;
use messenger::prelude::MaxTopicSize;
use predicates::str::{contains, starts_with};
use serial_test::parallel;

struct TestTopicListCmd {
    stream_id: u32,
    stream_name: String,
    topic_id: u32,
    topic_name: String,
    using_stream_id: TestStreamId,
    output: OutputFormat,
}

impl TestTopicListCmd {
    fn new(
        stream_id: u32,
        stream_name: String,
        topic_id: u32,
        topic_name: String,
        using_stream_id: TestStreamId,
        output: OutputFormat,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            using_stream_id,
            output,
        }
    }

    fn to_args(&self) -> Vec<String> {
        let mut args = match self.using_stream_id {
            TestStreamId::Numeric => vec![format!("{}", self.stream_id)],
            TestStreamId::Named => vec![self.stream_name.clone()],
        };

        args.extend(self.output.to_args().into_iter().map(String::from));

        args
    }
}

#[async_trait]
impl MessengerCmdTestCase for TestTopicListCmd {
    async fn prepare_server_state(&mut self, client: &dyn Client) {
        let stream = client
            .create_stream(&self.stream_name, Some(self.stream_id))
            .await;
        assert!(stream.is_ok());

        let topic = client
            .create_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_name,
                1,
                Default::default(),
                None,
                Some(self.topic_id),
                MessengerExpiry::NeverExpire,
                MaxTopicSize::ServerDefault,
            )
            .await;
        assert!(topic.is_ok());
    }

    fn get_command(&self) -> MessengerCmdCommand {
        MessengerCmdCommand::new()
            .arg("topic")
            .arg("list")
            .args(self.to_args())
            .with_env_credentials()
    }

    fn verify_command(&self, command_state: Assert) {
        let stream_id = match self.using_stream_id {
            TestStreamId::Numeric => format!("{}", self.stream_id),
            TestStreamId::Named => self.stream_name.clone(),
        };

        command_state
            .success()
            .stdout(starts_with(format!(
                "Executing list topics from stream with ID: {} in {} mode",
                stream_id, self.output
            )))
            .stdout(contains(self.topic_name.clone()));
    }

    async fn verify_server_state(&self, client: &dyn Client) {
        let topic = client
            .delete_topic(
                &self.stream_id.try_into().unwrap(),
                &self.topic_id.try_into().unwrap(),
            )
            .await;
        assert!(topic.is_ok());

        let stream = client
            .delete_stream(&self.stream_id.try_into().unwrap())
            .await;
        assert!(stream.is_ok());
    }
}

#[tokio::test]
#[parallel]
pub async fn should_be_successful() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test.setup().await;
    messenger_cmd_test
        .execute_test(TestTopicListCmd::new(
            1,
            String::from("main"),
            1,
            String::from("sync"),
            TestStreamId::Numeric,
            OutputFormat::Default,
        ))
        .await;
    messenger_cmd_test
        .execute_test(TestTopicListCmd::new(
            2,
            String::from("customer"),
            3,
            String::from("topic"),
            TestStreamId::Named,
            OutputFormat::List,
        ))
        .await;
    messenger_cmd_test
        .execute_test(TestTopicListCmd::new(
            3,
            String::from("production"),
            1,
            String::from("data"),
            TestStreamId::Numeric,
            OutputFormat::Table,
        ))
        .await;
}

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut messenger_cmd_test = MessengerCmdTest::help_message();

    messenger_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["topic", "list", "--help"],
            format!(
                r#"List all topics in given stream ID

Stream ID can be specified as a stream name or ID

Examples
 messenger topic list 1
 messenger topic list prod

{USAGE_PREFIX} topic list [OPTIONS] <STREAM_ID>

Arguments:
  <STREAM_ID>
          Stream ID to list topics
{CLAP_INDENT}
          Stream ID can be specified as a stream name or ID

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
            vec!["topic", "list", "-h"],
            format!(
                r#"List all topics in given stream ID

{USAGE_PREFIX} topic list [OPTIONS] <STREAM_ID>

Arguments:
  <STREAM_ID>  Stream ID to list topics

Options:
  -l, --list-mode <LIST_MODE>  List mode (table or list) [default: table] [possible values: table, list]
  -h, --help                   Print help (see more with '--help')
"#,
            ),
        ))
        .await;
}
