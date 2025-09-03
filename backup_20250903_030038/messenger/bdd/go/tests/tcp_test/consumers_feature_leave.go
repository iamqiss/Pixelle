// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tcp_test

import (
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/onsi/ginkgo/v2"
)

var _ = ginkgo.Describe("LEAVE CONSUMER GROUP:", func() {
	prefix := "LeaveConsumerGroup"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to leave consumer group, that he is a part of", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			groupId, _ := successfullyCreateConsumer(streamId, topicId, client)
			successfullyJoinConsumer(streamId, topicId, groupId, client)

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			groupIdentifier, _ := iggcon.NewIdentifier(groupId)
			err := client.LeaveConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				groupIdentifier,
			)

			itShouldNotReturnError(err)
			itShouldSuccessfullyLeaveConsumer(streamId, topicId, groupId, client)
		})

		ginkgo.Context("and tries to leave non-existing consumer group", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			err := client.LeaveConsumerGroup(
				streamIdentifier,
				topicIdentifier,
				randomU32Identifier(),
			)

			itShouldReturnSpecificError(err, "consumer_group_not_found")
		})

		ginkgo.Context("and tries to leave consumer non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			err := client.LeaveConsumerGroup(
				streamIdentifier,
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnSpecificError(err, "topic_id_not_found")
		})

		ginkgo.Context("and tries to leave consumer for non-existing topic and stream", func() {
			client := createAuthorizedConnection()

			err := client.LeaveConsumerGroup(
				randomU32Identifier(),
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to leave to the consumer group", func() {
			client := createClient()
			err := client.LeaveConsumerGroup(
				randomU32Identifier(),
				randomU32Identifier(),
				randomU32Identifier(),
			)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
