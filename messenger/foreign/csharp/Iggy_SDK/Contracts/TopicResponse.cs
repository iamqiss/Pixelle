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


using System.Text.Json.Serialization;
using Apache.Iggy.Enums;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Contracts;

public sealed class TopicResponse
{
    public required uint Id { get; init; }

    [JsonConverter(typeof(DateTimeOffsetConverter))]
    public required DateTimeOffset CreatedAt { get; init; }

    public required string Name { get; init; }
    public CompressionAlgorithm CompressionAlgorithm { get; set; }

    [JsonConverter(typeof(SizeConverter))]
    public required ulong Size { get; init; }

    public ulong MessageExpiry { get; init; }
    public required ulong MaxTopicSize { get; init; }
    public required ulong MessagesCount { get; init; }
    public required uint PartitionsCount { get; init; }
    public required byte? ReplicationFactor { get; init; }
    public IEnumerable<PartitionResponse>? Partitions { get; init; }
}