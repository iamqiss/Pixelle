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
	iggcon "github.com/apache/messenger/foreign/go/contracts"
	ierror "github.com/apache/messenger/foreign/go/errors"
	"github.com/onsi/ginkgo/v2"
)

var _ = ginkgo.Describe("GET TOPIC BY ID:", func() {
	prefix := "GetTopic"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to get existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, name := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicIdentifier, _ := iggcon.NewIdentifier(topicId)
			topic, err := client.GetTopic(streamIdentifier, topicIdentifier)

			itShouldNotReturnError(err)
			itShouldReturnSpecificTopic(topicId, name, *topic)
		})

		ginkgo.Context("and tries to get topic from non-existing stream", func() {
			client := createAuthorizedConnection()

			_, err := client.GetTopic(randomU32Identifier(), randomU32Identifier())

			itShouldReturnSpecificMessengerError(err, ierror.TopicIdNotFound)
		})

		ginkgo.Context("and tries to get non-existing topic", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)

			_, err := client.GetTopic(streamIdentifier, randomU32Identifier())

			itShouldReturnSpecificMessengerError(err, ierror.TopicIdNotFound)
		})
	})

	// ! TODO: review if needed to implement into sdk
	// When("User is not logged in", func() {
	// 	Context("and tries to get topic by id", func() {
	// 		client := createConnection()
	// 		_, err := client.GetTopicById(iggcon.NewIdentifier(int(createRandomUInt32())), iggcon.NewIdentifier(int(createRandomUInt32())))

	// 		itShouldReturnUnauthenticatedError(err)
	// 	})
	// })
})
