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

use crate::cli::common::{MessengerCmdTest, help::TestHelpCmd};
use serial_test::parallel;

const FIGLET_INDENT: &str = " ";
const FIGLET_FILL: &str = "                         ";

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut messenger_cmd_test = MessengerCmdTest::help_message();
    let no_arg: Vec<String> = vec![];

    messenger_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            no_arg,
            format!(
                r#"  ___                              ____   _       ___{FIGLET_INDENT}
 |_ _|   __ _    __ _   _   _     / ___| | |     |_ _|
  | |   / _` |  / _` | | | | |   | |     | |      | |{FIGLET_INDENT}
  | |  | (_| | | (_| | | |_| |   | |___  | |___   | |{FIGLET_INDENT}
 |___|  \__, |  \__, |  \__, |    \____| |_____| |___|
        |___/   |___/   |___/{FIGLET_FILL}

CLI for Messenger message streaming platform

Usage: messenger [OPTIONS] [COMMAND]

Commands:
  stream           stream operations [aliases: s]
  topic            topic operations [aliases: t]
  partition        partition operations [aliases: p]
  segment          segments operations [aliases: seg]
  ping             ping messenger server
  me               get current client info
  stats            get messenger server statistics
  snapshot         collect messenger server troubleshooting data
  pat              personal access token operations
  user             user operations [aliases: u]
  client           client operations [aliases: c]
  consumer-group   consumer group operations [aliases: g]
  consumer-offset  consumer offset operations [aliases: o]
  message          message operations [aliases: m]
  context          context operations [aliases: ctx]
  login            login to Messenger server [aliases: li]
  logout           logout from Messenger server [aliases: lo]
  help             Print this message or the help of the given subcommand(s)


Run 'messenger --help' for full help message.
Run 'messenger COMMAND --help' for more information on a command.

For more help on what's Messenger and how to use it, head to https://messenger.apache.org
"#,
            ),
        ))
        .await;
}
