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

package org.apache.messenger.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.messenger.client.blocking.tcp.MessengerTcpClient;
import org.apache.messenger.identifier.StreamId;
import org.apache.messenger.identifier.TopicId;
import org.apache.messenger.message.Message;
import org.apache.messenger.message.Partitioning;
import org.apache.messenger.stream.StreamDetails;
import org.apache.messenger.topic.CompressionAlgorithm;
import org.apache.messenger.topic.TopicDetails;
import java.math.BigInteger;
import java.util.Optional;
import static java.util.Collections.singletonList;
import static java.util.Optional.empty;

public class SimpleProducer {
    private static final String STREAM_NAME = "dev01";
    private static final StreamId STREAM_ID = StreamId.of(STREAM_NAME);
    private static final String TOPIC_NAME = "events";
    private static final TopicId TOPIC_ID = TopicId.of(TOPIC_NAME);
    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);

    public static void main(String[] args) {
        var client = new MessengerTcpClient("localhost", 8090);
        client.users().login("messenger", "messenger");

        createStream(client);
        createTopic(client);

        int counter = 0;
        while (counter++ < 1000) {
            var message = Message.of("message from simple producer " + counter);
            client.messages().sendMessages(STREAM_ID, TOPIC_ID, Partitioning.balanced(), singletonList(message));
            log.debug("Message {} sent", counter);
        }

    }

    private static void createStream(MessengerTcpClient client) {
        Optional<StreamDetails> stream = client.streams().getStream(STREAM_ID);
        if (stream.isPresent()) {
            return;
        }
        client.streams().createStream(empty(), STREAM_NAME);
    }

    private static void createTopic(MessengerTcpClient client) {
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

}
