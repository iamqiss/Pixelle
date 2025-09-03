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

using System.Net.Sockets;

namespace Apache.Iggy.ConnectionStream;

public sealed class TcpConnectionStream : IConnectionStream
{
    private readonly NetworkStream _stream;

    public TcpConnectionStream(NetworkStream stream)
    {
        _stream = stream;
    }

    public ValueTask SendAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken = default)
    {
        return _stream.WriteAsync(payload, cancellationToken);
    }

    public ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        return _stream.ReadAsync(buffer, cancellationToken);
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return _stream.FlushAsync(cancellationToken);
    }

    public void Close()
    {
        _stream.Close();
    }

    public void Dispose()
    {
        _stream.Dispose();
    }
}