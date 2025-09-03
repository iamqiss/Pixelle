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

package iggcon

import (
	ierror "github.com/apache/messenger/foreign/go/errors"
)

const (
	// MaxPayloadSize is maximum allowed size in bytes for a message payload.
	//
	// This constant defines the upper limit for the size of an MessengerMessage payload. Attempting to create a message
	// with a payload larger than this value will result
	// in an ierror.TooBigUserMessagePayload error.
	//
	//  Constraints
	//  - Minimum payload size: 1 byte (empty payloads are not allowed)
	//  - Maximum payload size: 10 MB
	MaxPayloadSize = 10 * 1000 * 1000

	// MaxUserHeadersSize is maximum allowed size in bytes for user-defined headers.
	//
	// This constant defines the upper limit for the combined size of all user headers in an MessengerMessage. Attempting to
	// create a message with user headers larger than this value will result in an ierror.TooBigUserHeaders error.
	//
	//  Constraints
	//  - Maximum headers size: 100 KB
	//  - Each individual header key is limited to 255 bytes
	//  - Each individual header value is limited to 255 bytes
	MaxUserHeadersSize = 100 * 1000
)

type PollMessageRequest struct {
	StreamId        Identifier      `json:"streamId"`
	TopicId         Identifier      `json:"topicId"`
	Consumer        Consumer        `json:"consumer"`
	PartitionId     uint32          `json:"partitionId"`
	PollingStrategy PollingStrategy `json:"pollingStrategy"`
	Count           int             `json:"count"`
	AutoCommit      bool            `json:"autoCommit"`
}

type PolledMessage struct {
	PartitionId   uint32
	CurrentOffset uint64
	MessageCount  uint32
	Messages      []MessengerMessage
}

type SendMessagesRequest struct {
	StreamId     Identifier    `json:"streamId"`
	TopicId      Identifier    `json:"topicId"`
	Partitioning Partitioning  `json:"partitioning"`
	Messages     []MessengerMessage `json:"messages"`
}

type ReceivedMessage struct {
	Message       MessengerMessage
	CurrentOffset uint64
	PartitionId   uint32
}

type MessengerMessage struct {
	Header      MessageHeader
	Payload     []byte
	UserHeaders []byte
}

type MessengerMessageOpt func(message *MessengerMessage)

// NewMessengerMessage Creates a new message with customizable parameters.
func NewMessengerMessage(payload []byte, opts ...MessengerMessageOpt) (MessengerMessage, error) {
	if len(payload) == 0 {
		return MessengerMessage{}, ierror.InvalidMessagePayloadLength
	}

	if len(payload) > MaxPayloadSize {
		return MessengerMessage{}, ierror.TooBigUserMessagePayload
	}

	header := NewMessageHeader(MessageID{}, uint32(len(payload)), 0)
	message := MessengerMessage{
		Header:      header,
		Payload:     payload,
		UserHeaders: make([]byte, 0),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&message)
		}
	}
	userHeaderLength := len(message.UserHeaders)
	if userHeaderLength > MaxUserHeadersSize {
		return MessengerMessage{}, ierror.TooBigUserHeaders
	}
	message.Header.UserHeaderLength = uint32(userHeaderLength)
	return message, nil
}

func WithID(id [16]byte) MessengerMessageOpt {
	return func(m *MessengerMessage) {
		m.Header.Id = id
	}
}

func WithUserHeaders(userHeaders map[HeaderKey]HeaderValue) MessengerMessageOpt {
	return func(m *MessengerMessage) {
		userHeaderBytes := GetHeadersBytes(userHeaders)
		m.UserHeaders = userHeaderBytes
	}
}
