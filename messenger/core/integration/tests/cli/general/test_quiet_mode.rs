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

use crate::cli::common::{MessengerCmdCommand, MessengerCmdTest, MessengerCmdTestCase};
use assert_cmd::assert::Assert;
use async_trait::async_trait;
use messenger::prelude::Client;
use predicates::str::diff;
use serial_test::parallel;

struct TestQuietModCmd {}

#[async_trait]
impl MessengerCmdTestCase for TestQuietModCmd {
    async fn prepare_server_state(&mut self, _client: &dyn Client) {}

    fn get_command(&self) -> MessengerCmdCommand {
        MessengerCmdCommand::new().arg("ping").opt("-q")
    }

    fn verify_command(&self, command_state: Assert) {
        command_state.success().stdout(diff(""));
    }

    async fn verify_server_state(&self, _client: &dyn Client) {}
}

#[tokio::test]
#[parallel]
pub async fn should_be_no_output() {
    let mut messenger_cmd_test = MessengerCmdTest::default();

    messenger_cmd_test.setup().await;
    messenger_cmd_test.execute_test(TestQuietModCmd {}).await;
}
