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

package binaryserialization

import (
	"encoding/binary"

	iggcon "github.com/apache/messenger/foreign/go/contracts"
	"github.com/klauspost/compress/s2"
)

type TcpSendMessagesRequest struct {
	StreamId     iggcon.Identifier    `json:"streamId"`
	TopicId      iggcon.Identifier    `json:"topicId"`
	Partitioning iggcon.Partitioning  `json:"partitioning"`
	Messages     []iggcon.MessengerMessage `json:"messages"`
}

const indexSize = 16

func (request *TcpSendMessagesRequest) Serialize(compression iggcon.MessengerMessageCompression) []byte {
	for i, message := range request.Messages {
		switch compression {
		case iggcon.MESSAGE_COMPRESSION_S2:
			if len(message.Payload) < 32 {
				break
			}
			request.Messages[i].Payload = s2.Encode(nil, message.Payload)
			message.Header.PayloadLength = uint32(len(message.Payload))
		case iggcon.MESSAGE_COMPRESSION_S2_BETTER:
			if len(message.Payload) < 32 {
				break
			}
			request.Messages[i].Payload = s2.EncodeBetter(nil, message.Payload)
			message.Header.PayloadLength = uint32(len(message.Payload))
		case iggcon.MESSAGE_COMPRESSION_S2_BEST:
			if len(message.Payload) < 32 {
				break
			}
			request.Messages[i].Payload = s2.EncodeBest(nil, message.Payload)
			message.Header.PayloadLength = uint32(len(message.Payload))
		}
	}

	streamIdFieldSize := 2 + request.StreamId.Length
	topicIdFieldSize := 2 + request.TopicId.Length
	partitioningFieldSize := 2 + request.Partitioning.Length
	metadataLenFieldSize := 4 // uint32
	messageCount := len(request.Messages)
	messagesCountFieldSize := 4 // uint32
	metadataLen := streamIdFieldSize +
		topicIdFieldSize +
		partitioningFieldSize +
		messagesCountFieldSize
	indexesSize := messageCount * indexSize
	messageBytesCount := calculateMessageBytesCount(request.Messages)
	totalSize := metadataLenFieldSize +
		streamIdFieldSize +
		topicIdFieldSize +
		partitioningFieldSize +
		messagesCountFieldSize +
		indexesSize +
		messageBytesCount

	bytes := make([]byte, totalSize)

	position := 0

	//metadata
	binary.LittleEndian.PutUint32(bytes[:4], uint32(metadataLen))
	position = 4
	//ids
	copy(bytes[position:position+streamIdFieldSize], SerializeIdentifier(request.StreamId))
	copy(bytes[position+streamIdFieldSize:position+streamIdFieldSize+topicIdFieldSize], SerializeIdentifier(request.TopicId))
	position += streamIdFieldSize + topicIdFieldSize

	//partitioning
	bytes[position] = byte(request.Partitioning.Kind)
	bytes[position+1] = byte(request.Partitioning.Length)
	copy(bytes[position+2:position+partitioningFieldSize], []byte(request.Partitioning.Value))
	position += partitioningFieldSize
	binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(messageCount))
	position += 4

	currentIndexPosition := position
	for i := 0; i < indexesSize; i++ {
		bytes[position+i] = 0
	}
	position += indexesSize

	msgSize := uint32(0)
	for _, message := range request.Messages {
		copy(bytes[position:position+iggcon.MessageHeaderSize], message.Header.ToBytes())
		copy(bytes[position+iggcon.MessageHeaderSize:position+iggcon.MessageHeaderSize+int(message.Header.PayloadLength)], message.Payload)
		position += iggcon.MessageHeaderSize + int(message.Header.PayloadLength)
		copy(bytes[position:position+int(message.Header.UserHeaderLength)], message.UserHeaders)
		position += int(message.Header.UserHeaderLength)

		msgSize += iggcon.MessageHeaderSize + message.Header.PayloadLength + message.Header.UserHeaderLength

		binary.LittleEndian.PutUint32(bytes[currentIndexPosition:currentIndexPosition+4], 0)
		binary.LittleEndian.PutUint32(bytes[currentIndexPosition+4:currentIndexPosition+8], uint32(msgSize))
		binary.LittleEndian.PutUint32(bytes[currentIndexPosition+8:currentIndexPosition+12], 0)
		currentIndexPosition += indexSize
	}

	return bytes
}

func calculateMessageBytesCount(messages []iggcon.MessengerMessage) int {
	count := 0
	for _, msg := range messages {
		count += iggcon.MessageHeaderSize + len(msg.Payload) + len(msg.UserHeaders)
	}
	return count
}
