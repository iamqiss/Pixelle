// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using Apache.Messenger.Contracts;
using Apache.Messenger.Enums;
using Apache.Messenger.Kinds;
using Apache.Messenger.Tests.Integrations.Fixtures;
using Apache.Messenger.Tests.Integrations.Models;
using Shouldly;

namespace Apache.Messenger.Tests.Integrations;

public class PollMessagesTests
{
    [ClassDataSource<PollMessagesFixture>(Shared = SharedType.PerClass)]
    public required PollMessagesFixture Fixture { get; init; }

    [Test]
    [Timeout(60_000)]
    [MethodDataSource<MessengerServerFixture>(nameof(MessengerServerFixture.ProtocolData))]
    public async Task PollMessagesTMessage_Should_PollMessages_Successfully(Protocol protocol, CancellationToken token)
    {
        var messageCount = 0;
        await foreach (MessageResponse<DummyMessage> msgResponse in Fixture.Clients[protocol].PollMessagesAsync(
                           new PollMessagesRequest
                           {
                               Consumer = Consumer.New(1),
                               Count = 10,
                               PartitionId = 1,
                               PollingStrategy = PollingStrategy.Next(),
                               StreamId = Identifier.Numeric(Fixture.StreamId),
                               TopicId = Identifier.Numeric(Fixture.TopicRequest.TopicId!.Value)
                           }, DummyMessage.DeserializeDummyMessage, token: token))
        {
            msgResponse.UserHeaders.ShouldNotBeNull();
            msgResponse.UserHeaders.Count.ShouldBe(2);
            msgResponse.Message.Text.ShouldContain("Dummy message");
            messageCount++;
            if (messageCount == Fixture.MessageCount)
            {
                break;
            }
        }

        messageCount.ShouldBe(Fixture.MessageCount);
    }
}