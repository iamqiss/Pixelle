# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import asyncio
from collections import namedtuple

from apache_iggy import IggyClient, PollingStrategy, ReceiveMessage
from loguru import logger

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
STREAM_ID = 1
TOPIC_ID = 1
PARTITION_ID = 1
BATCHES_LIMIT = 5

ArgNamespace = namedtuple("ArgNamespace", ["connection_string"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "connection_string",
        help=(
            "Connection string for Iggy client, e.g. 'iggy+tcp://iggy:iggy@127.0.0.1:8090'"
        ),
        default="iggy+tcp://iggy:iggy@127.0.0.1:8090",
        type=str,
    )
    return parser.parse_args()


async def main():
    args: ArgNamespace = parse_args()
    client = IggyClient.from_connection_string(args.connection_string)
    logger.info("Connecting to Iggy")
    await client.connect()
    logger.info("Connected")
    await consume_messages(client)


async def consume_messages(client: IggyClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    logger.info(
        f"Messages will be consumed from stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with "
        f"interval {interval * 1000} ms."
    )
    offset = 0
    messages_per_batch = 10
    n_consumed_batches = 0
    while n_consumed_batches < BATCHES_LIMIT:
        try:
            logger.debug("Polling for messages...")
            polled_messages = await client.poll_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partition_id=PARTITION_ID,
                polling_strategy=PollingStrategy.Next(),
                count=messages_per_batch,
                auto_commit=True,
            )
            if not polled_messages:
                logger.info("No messages found in current poll")
                await asyncio.sleep(interval)
                continue

            offset += len(polled_messages)
            for message in polled_messages:
                handle_message(message)
            n_consumed_batches += 1
            await asyncio.sleep(interval)
        except Exception as error:
            logger.exception("Exception occurred while consuming messages: {}", error)
            break

    logger.info(f"Consumed {n_consumed_batches} batches of messages, exiting.")


def handle_message(message: ReceiveMessage):
    payload = message.payload().decode("utf-8")
    logger.info(
        f"Handling message at offset: {message.offset()} with payload: {payload}..."
    )


if __name__ == "__main__":
    asyncio.run(main())
