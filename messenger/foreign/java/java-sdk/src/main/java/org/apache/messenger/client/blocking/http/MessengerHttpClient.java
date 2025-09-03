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

package org.apache.messenger.client.blocking.http;

import org.apache.messenger.client.blocking.ConsumerGroupsClient;
import org.apache.messenger.client.blocking.ConsumerOffsetsClient;
import org.apache.messenger.client.blocking.MessengerBaseClient;
import org.apache.messenger.client.blocking.MessagesClient;
import org.apache.messenger.client.blocking.PartitionsClient;
import org.apache.messenger.client.blocking.PersonalAccessTokensClient;
import org.apache.messenger.client.blocking.StreamsClient;
import org.apache.messenger.client.blocking.SystemClient;
import org.apache.messenger.client.blocking.TopicsClient;
import org.apache.messenger.client.blocking.UsersClient;

public class MessengerHttpClient implements MessengerBaseClient {

    private final SystemHttpClient systemClient;
    private final StreamsHttpClient streamsClient;
    private final UsersHttpClient usersClient;
    private final TopicsHttpClient topicsClient;
    private final PartitionsHttpClient partitionsClient;
    private final ConsumerGroupsHttpClient consumerGroupsClient;
    private final ConsumerOffsetsHttpClient consumerOffsetsClient;
    private final MessagesHttpClient messagesClient;
    private final PersonalAccessTokensHttpClient personalAccessTokensHttpClient;

    public MessengerHttpClient(String url) {
        InternalHttpClient httpClient = new InternalHttpClient(url);
        systemClient = new SystemHttpClient(httpClient);
        streamsClient = new StreamsHttpClient(httpClient);
        usersClient = new UsersHttpClient(httpClient);
        topicsClient = new TopicsHttpClient(httpClient);
        partitionsClient = new PartitionsHttpClient(httpClient);
        consumerGroupsClient = new ConsumerGroupsHttpClient(httpClient);
        consumerOffsetsClient = new ConsumerOffsetsHttpClient(httpClient);
        messagesClient = new MessagesHttpClient(httpClient);
        personalAccessTokensHttpClient = new PersonalAccessTokensHttpClient(httpClient);
    }

    @Override
    public SystemClient system() {
        return systemClient;
    }

    @Override
    public StreamsClient streams() {
        return streamsClient;
    }

    @Override
    public UsersClient users() {
        return usersClient;
    }

    @Override
    public TopicsClient topics() {
        return topicsClient;
    }

    @Override
    public PartitionsClient partitions() {
        return partitionsClient;
    }

    @Override
    public ConsumerGroupsClient consumerGroups() {
        return consumerGroupsClient;
    }

    @Override
    public ConsumerOffsetsClient consumerOffsets() {
        return consumerOffsetsClient;
    }

    @Override
    public MessagesClient messages() {
        return messagesClient;
    }

    @Override
    public PersonalAccessTokensClient personalAccessTokens() {
        return personalAccessTokensHttpClient;
    }

}
