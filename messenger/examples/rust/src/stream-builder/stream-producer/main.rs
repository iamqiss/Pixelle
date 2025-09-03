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

use messenger::prelude::*;
use std::str::FromStr;

const MESSENGER_URL: &str = "messenger://messenger:messenger@localhost:8090";

#[tokio::main]
async fn main() -> Result<(), MessengerError> {
    println!("Build messenger client and producer");
    // For customization, use the `new` or `from_stream_topic` constructor
    let config = MessengerProducerConfig::default();
    let (client, producer) = MessengerStreamProducer::with_client_from_url(MESSENGER_URL, &config).await?;

    println!("Send 3 test messages...");
    producer
        .send_one(MessengerMessage::from_str("Hello World")?)
        .await?;
    producer
        .send_one(MessengerMessage::from_str("Hola Messenger")?)
        .await?;
    producer
        .send_one(MessengerMessage::from_str("Hi Apache")?)
        .await?;

    // Wait a bit for all messages to arrive.
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    println!("Stop the message stream and shutdown messenger client");
    client.shutdown().await?;
    Ok(())
}
