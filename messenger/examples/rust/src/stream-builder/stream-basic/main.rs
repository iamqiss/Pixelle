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

use crate::shared::stream::PrintEventConsumer;
use messenger::consumer_ext::MessengerConsumerMessageExt;
use messenger::prelude::*;
use messenger_examples::shared;
use std::str::FromStr;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> Result<(), MessengerError> {
    println!("Build messenger client and connect it.");
    let client = shared::client::build_client("test_stream", "test_topic", true).await?;

    println!("Build messenger producer & consumer");
    // For customization, use the `new` or `from_stream_topic` constructor
    let stream_config = MessengerStreamConfig::default();
    let (producer, mut consumer) = MessengerStream::build(&client, &stream_config).await?;

    println!("Start message stream");
    let (sender, receiver) = oneshot::channel();
    tokio::spawn(async move {
        match consumer
            // PrintEventConsumer is imported from examples/src/shared/stream.rs
            .consume_messages(&PrintEventConsumer {}, receiver)
            .await
        {
            Ok(_) => {}
            Err(err) => eprintln!("Failed to consume messages: {err}"),
        }
    });

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
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    println!("Stop the message stream and shutdown messenger client");
    sender.send(()).expect("Failed to send shutdown signal");
    client.delete_stream(stream_config.stream_id()).await?;
    client.shutdown().await?;

    Ok(())
}
