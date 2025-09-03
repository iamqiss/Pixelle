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

package ierror

var (
	ResourceNotFound = &MessengerError{
		Code:    20,
		Message: "resource_not_found",
	}
	InvalidConfiguration = &MessengerError{
		Code:    2,
		Message: "invalid_configuration",
	}
	InvalidIdentifier = &MessengerError{
		Code:    6,
		Message: "invalid_identifier",
	}
	StreamIdNotFound = &MessengerError{
		Code:    1009,
		Message: "stream_id_not_found",
	}
	TopicIdNotFound = &MessengerError{
		Code:    2010,
		Message: "topic_id_not_found",
	}
	InvalidMessagesCount = &MessengerError{
		Code:    4009,
		Message: "invalid_messages_count",
	}
	InvalidMessagePayloadLength = &MessengerError{
		Code:    4025,
		Message: "invalid_message_payload_length",
	}
	TooBigUserMessagePayload = &MessengerError{
		Code:    4022,
		Message: "too_big_message_payload",
	}
	TooBigUserHeaders = &MessengerError{
		Code:    4017,
		Message: "too_big_headers_payload",
	}
	ConsumerGroupIdNotFound = &MessengerError{
		Code:    5000,
		Message: "consumer_group_not_found",
	}
)
