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
import typing
import urllib
import urllib.parse
from collections import namedtuple

from apache_messenger import MessengerClient, StreamDetails, TopicDetails
from apache_messenger import SendMessage as Message
from loguru import logger

STREAM_NAME = "sample-stream"
TOPIC_NAME = "sample-topic"
STREAM_ID = 1
TOPIC_ID = 1
PARTITION_ID = 1
BATCHES_LIMIT = 5

ArgNamespace = namedtuple("ArgNamespace", ["tcp_server_address"])


class ValidateUrl(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: typing.List[typing.Any],
        option_string: typing.Optional[str] = None,
    ):
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse("//" + values)
        if parsed_url.netloc == "" or parsed_url.path != "":
            parser.error(f"Invalid server address: {values}")
        setattr(namespace, self.dest, values)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tcp-server-address",
        help="Messenger TCP server address (host:port)",
        action=ValidateUrl,
        default="127.0.0.1:8090",
    )
    return parser.parse_args()


async def main():
    args: ArgNamespace = parse_args()
    client = MessengerClient(args.tcp_server_address)
    logger.info("Connecting to MessengerClient")
    await client.connect()
    logger.info("Connected. Logging in user...")
    await client.login_user("messenger", "messenger")
    logger.info("Logged in.")
    await init_system(client)
    await produce_messages(client)


async def init_system(client: MessengerClient):
    try:
        logger.info(f"Creating stream with name {STREAM_NAME}...")
        stream: StreamDetails = await client.get_stream(STREAM_NAME)
        if stream is None:
            await client.create_stream(name=STREAM_NAME, stream_id=STREAM_ID)
            logger.info("Stream was created successfully.")
        else:
            logger.warning(f"Stream {stream.name} already exists with ID {stream.id}")

    except Exception as error:
        logger.error(f"Error creating stream: {error}")
        logger.exception(error)

    try:
        logger.info(f"Creating topic {TOPIC_NAME} in stream {STREAM_NAME}")
        topic: TopicDetails = await client.get_topic(STREAM_NAME, TOPIC_NAME)
        if topic is None:
            await client.create_topic(
                stream=STREAM_NAME,
                partitions_count=1,
                name=TOPIC_NAME,
                replication_factor=1,
            )
            logger.info("Topic was created successfully.")
        else:
            logger.warning(f"Topic {topic.name} already exists with ID {topic.id}")
    except Exception as error:
        logger.error(f"Error creating topic {error}")
        logger.exception(error)


async def produce_messages(client: MessengerClient):
    interval = 0.5  # 500 milliseconds in seconds for asyncio.sleep
    logger.info(
        f"Messages will be sent to stream: {STREAM_NAME}, topic: {TOPIC_NAME}, partition: {PARTITION_ID} with interval {interval * 1000} ms."
    )
    current_id = 0
    messages_per_batch = 10
    n_sent_batches = 0
    while n_sent_batches < BATCHES_LIMIT:
        messages = []
        for _ in range(messages_per_batch):
            current_id += 1
            payload = f"message-{current_id}"
            message = Message(payload)
            messages.append(message)
        logger.info(
            f"Attempting to send batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}"
        )
        try:
            await client.send_messages(
                stream=STREAM_NAME,
                topic=TOPIC_NAME,
                partitioning=PARTITION_ID,
                messages=messages,
            )
            n_sent_batches += 1
            logger.info(
                f"Successfully sent batch of {messages_per_batch} messages. Batch ID: {current_id // messages_per_batch}"
            )
        except Exception as error:
            logger.error(f"Exception type: {type(error).__name__}, message: {error}")
            logger.exception(error)

        await asyncio.sleep(interval)
    logger.info(f"Sent {n_sent_batches} batches of messages, exiting.")


if __name__ == "__main__":
    asyncio.run(main())
