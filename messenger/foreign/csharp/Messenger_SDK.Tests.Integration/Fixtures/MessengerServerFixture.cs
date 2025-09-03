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

using Apache.Messenger.Configuration;
using Apache.Messenger.Enums;
using Apache.Messenger.Factory;
using Apache.Messenger.MessengerClient;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Logging.Abstractions;
using TUnit.Core.Interfaces;

namespace Apache.Messenger.Tests.Integrations.Fixtures;

public class MessengerServerFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly IContainer _httpContainer = new ContainerBuilder().WithImage("apache/messenger:edge")
        // Container name is just to be used locally for debbuging effects
        //.WithName($"SutMessengerContainerHTTP")
        .WithPortBinding(3000, true)
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(3000))
        .WithName($"HTTP_{Guid.NewGuid()}")
        //.WithEnvironment("MESSENGER_SYSTEM_LOGGING_LEVEL", "trace")
        //.WithEnvironment("RUST_LOG", "trace")
        .WithCleanUp(true)
        .Build();

    private readonly IContainer _tcpContainer = new ContainerBuilder().WithImage("apache/messenger:edge")
        // Container name is just to be used locally for debbuging effects
        //.WithName($"SutMessengerContainerTCP")
        .WithPortBinding(3000, true)
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8090))
        .WithName($"TCP_{Guid.NewGuid()}")
        //.WithEnvironment("MESSENGER_SYSTEM_LOGGING_LEVEL", "trace")
        //.WithEnvironment("RUST_LOG", "trace")
        .WithCleanUp(true)
        .Build();

    public Dictionary<Protocol, IMessengerClient> Clients { get; } = new();

    private Action<MessageBatchingSettings> BatchingSettings { get; } = options =>
    {
        options.Enabled = false;
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 4096;
    };

    private Action<MessagePollingSettings> PollingSettings { get; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.StoreOffsetStrategy = StoreOffset.WhenMessagesAreReceived;
    };

    public async ValueTask DisposeAsync()
    {
        await Task.WhenAll(_tcpContainer.StopAsync(), _httpContainer.StopAsync());
    }

    public virtual async Task InitializeAsync()
    {
        await Task.WhenAll(_tcpContainer.StartAsync(), _httpContainer.StartAsync());

        await CreateTcpClient();
        await CreateHttpClient();
    }

    public async Task CreateTcpClient()
    {
        var tcpClient = CreateClient(Protocol.Tcp);

        await tcpClient.LoginUser("messenger", "messenger");

        Clients[Protocol.Tcp] = tcpClient;
    }

    public async Task CreateHttpClient()
    {
        var client = CreateClient(Protocol.Http);

        await client.LoginUser("messenger", "messenger");

        Clients[Protocol.Http] = client;
    }

    public IMessengerClient CreateClient(Protocol protocol, Protocol? targetContainer = null)
    {
        var port = protocol == Protocol.Tcp
            ? _tcpContainer.GetMappedPublicPort(8090)
            : _httpContainer.GetMappedPublicPort(3000);

        if (targetContainer != null
            && targetContainer != protocol)
        {
            port = targetContainer == Protocol.Tcp
                ? _tcpContainer.GetMappedPublicPort(3000)
                : _httpContainer.GetMappedPublicPort(8090);
        }

        var address = protocol == Protocol.Tcp
            ? $"127.0.0.1:{port}"
            : $"http://127.0.0.1:{port}";

        return MessageStreamFactory.CreateMessageStream(options =>
        {
            options.BaseAdress = address;
            options.Protocol = protocol;
            options.MessageBatchingSettings = BatchingSettings;
            options.MessagePollingSettings = PollingSettings;
        }, NullLoggerFactory.Instance);
    }

    public static IEnumerable<Func<Protocol>> ProtocolData()
    {
        yield return () => Protocol.Http;
        yield return () => Protocol.Tcp;
    }
}