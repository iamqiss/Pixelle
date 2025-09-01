/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.transport.netty4;

import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.PageCacheRecycler;
import org.density.common.util.io.IOUtils;
import org.density.common.util.net.NetUtils;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.transport.TransportAddress;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.transport.MockTransportService;
import org.density.test.transport.StubbableTransport;
import org.density.transport.AbstractSimpleTransportTestCase;
import org.density.transport.ConnectTransportException;
import org.density.transport.ConnectionProfile;
import org.density.transport.Netty4NioSocketChannel;
import org.density.transport.SharedGroupFactory;
import org.density.transport.TcpChannel;
import org.density.transport.TcpTransport;
import org.density.transport.TestProfiles;
import org.density.transport.Transport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SimpleNetty4TransportTests extends AbstractSimpleTransportTestCase {

    @Override
    protected Transport build(Settings settings, final Version version, ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        return new Netty4Transport(
            settings,
            version,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            namedWriteableRegistry,
            new NoneCircuitBreakerService(),
            new SharedGroupFactory(settings),
            NoopTracer.INSTANCE
        ) {

            @Override
            public void executeHandshake(
                DiscoveryNode node,
                TcpChannel channel,
                ConnectionProfile profile,
                ActionListener<Version> listener
            ) {
                if (doHandshake) {
                    super.executeHandshake(node, channel, profile, listener);
                } else {
                    listener.onResponse(version.minimumCompatibilityVersion());
                }
            }
        };
    }

    public void testConnectException() throws UnknownHostException {
        try {
            serviceA.connectToNode(
                new DiscoveryNode(
                    "C",
                    new TransportAddress(InetAddress.getByName("localhost"), 9876),
                    emptyMap(),
                    emptySet(),
                    Version.CURRENT
                )
            );
            fail("Expected ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("connect_exception"));
            assertThat(e.getMessage(), containsString("[127.0.0.1:9876]"));
        }
    }

    public void testDefaultKeepAliveSettings() throws IOException {
        assumeTrue("setting default keepalive options not supported on this platform", (IOUtils.LINUX || IOUtils.MAC_OS_X));
        try (
            MockTransportService serviceC = buildService("TS_C", Version.CURRENT, Settings.EMPTY);
            MockTransportService serviceD = buildService("TS_D", Version.CURRENT, Settings.EMPTY)
        ) {
            serviceC.start();
            serviceC.acceptIncomingRequests();
            serviceD.start();
            serviceD.acceptIncomingRequests();

            try (Transport.Connection connection = serviceC.openConnection(serviceD.getLocalDiscoNode(), TestProfiles.LIGHT_PROFILE)) {
                assertThat(connection, instanceOf(StubbableTransport.WrappedConnection.class));
                Transport.Connection conn = ((StubbableTransport.WrappedConnection) connection).getConnection();
                assertThat(conn, instanceOf(TcpTransport.NodeChannels.class));
                TcpTransport.NodeChannels nodeChannels = (TcpTransport.NodeChannels) conn;
                for (TcpChannel channel : nodeChannels.getChannels()) {
                    assertFalse(channel.isServerChannel());
                    checkDefaultKeepAliveOptions(channel);
                }

                assertThat(serviceD.getOriginalTransport(), instanceOf(TcpTransport.class));
                for (TcpChannel channel : getAcceptedChannels((TcpTransport) serviceD.getOriginalTransport())) {
                    assertTrue(channel.isServerChannel());
                    checkDefaultKeepAliveOptions(channel);
                }
            }
        }
    }

    private void checkDefaultKeepAliveOptions(TcpChannel channel) throws IOException {
        assertThat(channel, instanceOf(Netty4TcpChannel.class));
        Netty4TcpChannel nettyChannel = (Netty4TcpChannel) channel;
        assertThat(nettyChannel.getNettyChannel(), instanceOf(Netty4NioSocketChannel.class));
        Netty4NioSocketChannel netty4NioSocketChannel = (Netty4NioSocketChannel) nettyChannel.getNettyChannel();
        SocketChannel socketChannel = netty4NioSocketChannel.javaChannel();
        assertThat(socketChannel.supportedOptions(), hasItem(NetUtils.getTcpKeepIdleSocketOptionOrNull()));
        Integer keepIdle = socketChannel.getOption(NetUtils.getTcpKeepIdleSocketOptionOrNull());
        assertNotNull(keepIdle);
        assertThat(keepIdle, lessThanOrEqualTo(500));
        assertThat(socketChannel.supportedOptions(), hasItem(NetUtils.getTcpKeepIntervalSocketOptionOrNull()));
        Integer keepInterval = socketChannel.getOption(NetUtils.getTcpKeepIntervalSocketOptionOrNull());
        assertNotNull(keepInterval);
        assertThat(keepInterval, lessThanOrEqualTo(500));
    }
}
