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
	iggcon "github.com/apache/messenger/foreign/go/contracts"
)

func SerializeIdentifier(identifier iggcon.Identifier) []byte {
	bytes := make([]byte, identifier.Length+2)
	bytes[0] = byte(identifier.Kind)
	bytes[1] = byte(identifier.Length)
	copy(bytes[2:], identifier.Value)
	return bytes
}

func SerializeIdentifiers(identifiers ...iggcon.Identifier) []byte {
	size := 0
	for i := 0; i < len(identifiers); i++ {
		size += 2 + identifiers[i].Length
	}
	bytes := make([]byte, size)
	position := 0

	for i := 0; i < len(identifiers); i++ {
		copy(bytes[position:position+2+identifiers[i].Length], SerializeIdentifier(identifiers[i]))
		position += 2 + identifiers[i].Length
	}

	return bytes
}
