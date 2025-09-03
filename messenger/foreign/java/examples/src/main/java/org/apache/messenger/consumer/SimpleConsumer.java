/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.messenger.consumer;

import org.apache.messenger.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.messenger.client.blocking.MessengerBaseClient;
import org.apache.messenger.client.blocking.tcp.MessengerTcpClient;
import org.apache.messenger.consumergroup.Consumer;
import org.apache.messenger.consumergroup.ConsumerGroupDetails;
import org.apache.messenger.identifier.ConsumerId;
import org.apache.messenger.identifier.StreamId;
import org.apache.messenger.identifier.TopicId;
import org.apache.messenger.message.PollingStrategy;
import org.apache.messenger.stream.StreamDetails;
import org.apache.messenger.topic.CompressionAlgorithm;
import org.apache.messenger.topic.TopicDetails;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Optional;
import static java.util.Optional.empty;

public class SimpleConsumer {

    private static final String STREAM_NAME = "dev01";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);
    private static final String TOPIC_NAME = "events";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);
    private static final String GROUP_NAME = "simple-consumer";
    private static final ConsumerId GROUP_ID = ConsumerId.of(GROUP_NAME);
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args) {
        var client = new MessengerTcpClient("localhost", 8090);
        client.users().login("messenger", "messenger");

        createStream(client);
        createTopic(client);
        createConsumerGroup(client);
        client.consumerGroups().joinConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_ID);

        var messages = new ArrayList<Message>();
        while (messages.size() < 1000) {
            var polledMessages = client.messages()
                    .pollMessages(STREAM_ID,
                            TOPIC_ID,
                            empty(),
                            Consumer.group(GROUP_ID),
                            PollingStrategy.next(),
                            10L,
                            true);
            messages.addAll(polledMessages.messages());
            log.debug("Fetched {} messages from partition {}, current offset {}",
                    polledMessages.messages().size(),
                    polledMessages.partitionId(),
                    polledMessages.currentOffset());
        }
    }

    private static void createStream(MessengerBaseClient client) {
        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isPresent()) {
            return;
        }
        client.streams().createStream(empty(), STREAM_NAME);
    }

    private static void createTopic(MessengerBaseClient client) {
        Optional<TopicDetails> topic = client.topics().getTopic(STREAM_ID, TOPIC_ID);
        if (topic.isPresent()) {
            return;
        }
        client.topics()
                .createTopic(STREAM_ID,
                        empty(),
                        1L,
                        CompressionAlgorithm.None,
                        BigInteger.ZERO,
                        BigInteger.ZERO,
                        empty(),
                        TOPIC_NAME);

    }

    private static void createConsumerGroup(MessengerBaseClient client) {
        Optional<ConsumerGroupDetails> consumerGroup = client.consumerGroups()
                .getConsumerGroup(STREAM_ID, TOPIC_ID, GROUP_ID);
        if (consumerGroup.isPresent()) {
            return;
        }
        client.consumerGroups().createConsumerGroup(STREAM_ID, TOPIC_ID, empty(), GROUP_NAME);
    }

}
