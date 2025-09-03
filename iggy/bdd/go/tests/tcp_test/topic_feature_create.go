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
	"math"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/onsi/ginkgo/v2"
)

var _ = ginkgo.Describe("CREATE TOPIC:", func() {
	prefix := "CreateTopic"
	ginkgo.When("User is logged in", func() {
		ginkgo.Context("and tries to create topic unique name and id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			topicId := uint32(1)
			replicationFactor := uint8(1)
			name := createRandomString(32)
			defer deleteStreamAfterTests(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			_, err := client.CreateTopic(
				streamIdentifier,
				name,
				2,
				iggcon.CompressionAlgorithmNone,
				iggcon.Millisecond,
				math.MaxUint64,
				&replicationFactor,
				&topicId)

			itShouldNotReturnError(err)
			itShouldSuccessfullyCreateTopic(streamId, topicId, name, client)
		})

		ginkgo.Context("and tries to create topic for a non existing stream", func() {
			client := createAuthorizedConnection()
			streamId := createRandomUInt32()
			topicId := uint32(1)
			replicationFactor := uint8(1)
			name := createRandomString(32)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			_, err := client.CreateTopic(
				streamIdentifier,
				name,
				2,
				iggcon.CompressionAlgorithmNone,
				iggcon.Millisecond,
				math.MaxUint64,
				&replicationFactor,
				&topicId)

			itShouldReturnSpecificError(err, "stream_id_not_found")
		})

		ginkgo.Context("and tries to create topic with duplicate topic name", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			_, name := successfullyCreateTopic(streamId, client)

			replicationFactor := uint8(1)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			topicId := createRandomUInt32()
			_, err := client.CreateTopic(
				streamIdentifier,
				name,
				2,
				iggcon.CompressionAlgorithmNone,
				iggcon.IggyExpiryServerDefault,
				math.MaxUint64,
				&replicationFactor,
				&topicId)
			itShouldReturnSpecificError(err, "topic_name_already_exists")
		})

		ginkgo.Context("and tries to create topic with duplicate topic id", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, client)
			topicId, _ := successfullyCreateTopic(streamId, client)
			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			replicationFactor := uint8(1)
			_, err := client.CreateTopic(
				streamIdentifier,
				createRandomString(32),
				2,
				iggcon.CompressionAlgorithmNone,
				iggcon.IggyExpiryServerDefault,
				math.MaxUint64,
				&replicationFactor,
				&topicId)
			itShouldReturnSpecificError(err, "topic_id_already_exists")
		})

		ginkgo.Context("and tries to create topic with name that's over 255 characters", func() {
			client := createAuthorizedConnection()
			streamId, _ := successfullyCreateStream(prefix, client)
			defer deleteStreamAfterTests(streamId, createAuthorizedConnection())

			streamIdentifier, _ := iggcon.NewIdentifier(streamId)
			replicationFactor := uint8(1)
			topicId := createRandomUInt32()
			_, err := client.CreateTopic(
				streamIdentifier,
				createRandomString(256),
				2,
				iggcon.CompressionAlgorithmNone,
				iggcon.IggyExpiryServerDefault,
				math.MaxUint64,
				&replicationFactor,
				&topicId)

			itShouldReturnSpecificError(err, "topic_name_too_long")
		})
	})

	ginkgo.When("User is not logged in", func() {
		ginkgo.Context("and tries to create topic", func() {
			client := createClient()
			replicationFactor := uint8(1)
			topicId := uint32(1)
			streamIdentifier, _ := iggcon.NewIdentifier[uint32](10)
			_, err := client.CreateTopic(
				streamIdentifier,
				"name",
				2,
				iggcon.CompressionAlgorithmNone,
				iggcon.IggyExpiryServerDefault,
				math.MaxUint64,
				&replicationFactor,
				&topicId)

			itShouldReturnUnauthenticatedError(err)
		})
	})
})
