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

use messenger::clients::consumer::ReceivedMessage;
use messenger::consumer_ext::MessageConsumer;
use messenger::prelude::MessengerError;

#[derive(Debug)]
pub struct PrintEventConsumer {}

impl MessageConsumer for PrintEventConsumer {
    async fn consume(&self, message: ReceivedMessage) -> Result<(), MessengerError> {
        // Extract message payload as raw bytes
        let raw_message = message.message.payload.as_ref();
        // Convert raw bytes into string
        let payload = String::from_utf8_lossy(raw_message);
        // Print message
        println!(
            "Message received: {} at offset: {} in partition ID: {}",
            payload, message.message.header.offset, message.partition_id
        );

        Ok(())
    }
}
