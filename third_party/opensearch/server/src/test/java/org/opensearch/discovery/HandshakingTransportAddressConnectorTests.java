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

package org.density.discovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.DensityException;
import org.density.Version;
import org.density.cluster.ClusterName;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.Nullable;
import org.density.common.SetOnce;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.transport.TransportAddress;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.MockLogAppender;
import org.density.test.DensityTestCase;
import org.density.test.junit.annotations.TestLogging;
import org.density.test.transport.MockTransport;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.ConnectTransportException;
import org.density.transport.TransportException;
import org.density.transport.TransportRequest;
import org.density.transport.TransportService;
import org.density.transport.TransportService.HandshakeResponse;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.density.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.density.discovery.HandshakingTransportAddressConnector.PROBE_HANDSHAKE_TIMEOUT_SETTING;
import static org.density.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

public class HandshakingTransportAddressConnectorTests extends DensityTestCase {

    private DiscoveryNode remoteNode;
    private TransportAddress discoveryAddress;
    private TransportService transportService;
    private ThreadPool threadPool;
    private String remoteClusterName;
    private HandshakingTransportAddressConnector handshakingTransportAddressConnector;
    private DiscoveryNode localNode;

    private boolean dropHandshake;
    @Nullable // unless we want the full connection to fail
    private TransportException fullConnectionFailure;

    @Before
    public void startServices() {
        localNode = new DiscoveryNode("local-node", buildNewFakeTransportAddress(), Version.CURRENT);
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(CLUSTER_NAME_SETTING.getKey(), "local-cluster")
            .build();
        threadPool = new TestThreadPool("node", settings);

        remoteNode = null;
        discoveryAddress = null;
        remoteClusterName = null;
        dropHandshake = false;
        fullConnectionFailure = null;

        final MockTransport mockTransport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                super.onSendRequest(requestId, action, request, node);
                assertThat(action, equalTo(TransportService.HANDSHAKE_ACTION_NAME));
                assertThat(discoveryAddress, notNullValue());
                assertThat(node.getAddress(), oneOf(discoveryAddress, remoteNode.getAddress()));
                if (dropHandshake == false) {
                    if (fullConnectionFailure != null && node.getAddress().equals(remoteNode.getAddress())) {
                        handleError(requestId, fullConnectionFailure);
                    } else {
                        handleResponse(requestId, new HandshakeResponse(remoteNode, new ClusterName(remoteClusterName), Version.CURRENT));
                    }
                }
            }
        };

        transportService = mockTransport.createTransportService(
            settings,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> localNode,
            null,
            emptySet(),
            NoopTracer.INSTANCE
        );

        transportService.start();
        transportService.acceptIncomingRequests();

        handshakingTransportAddressConnector = new HandshakingTransportAddressConnector(settings, transportService);
    }

    @After
    public void stopServices() {
        transportService.stop();
        terminate(threadPool);
    }

    public void testConnectsToClustreManagerNode() throws InterruptedException {
        final CountDownLatch completionLatch = new CountDownLatch(1);
        final SetOnce<DiscoveryNode> receivedNode = new SetOnce<>();

        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        remoteClusterName = "local-cluster";
        discoveryAddress = getDiscoveryAddress();

        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, new ActionListener<DiscoveryNode>() {
            @Override
            public void onResponse(DiscoveryNode discoveryNode) {
                receivedNode.set(discoveryNode);
                completionLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
        assertEquals(remoteNode, receivedNode.get());
    }

    @TestLogging(reason = "ensure logging happens", value = "org.density.discovery.HandshakingTransportAddressConnector:INFO")
    public void testLogsFullConnectionFailureAfterSuccessfulHandshake() throws Exception {

        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        remoteClusterName = "local-cluster";
        discoveryAddress = buildNewFakeTransportAddress();

        fullConnectionFailure = new ConnectTransportException(remoteNode, "simulated", new DensityException("root cause"));

        FailureListener failureListener = new FailureListener();

        Logger targetLogger = LogManager.getLogger(HandshakingTransportAddressConnector.class);
        try (MockLogAppender mockAppender = MockLogAppender.createForLoggers(targetLogger)) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "message",
                    HandshakingTransportAddressConnector.class.getCanonicalName(),
                    Level.WARN,
                    "*completed handshake with [*] but followup connection failed*"
                )
            );

            handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
            failureListener.assertFailure();
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testDoesNotConnectToNonClusterManagerNode() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        failureListener.assertFailure();
    }

    public void testDoesNotConnectToLocalNode() throws Exception {
        remoteNode = localNode;
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        failureListener.assertFailure();
    }

    public void testDoesNotConnectToDifferentCluster() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "another-cluster";

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        failureListener.assertFailure();
    }

    public void testHandshakeTimesOut() throws InterruptedException {
        remoteNode = new DiscoveryNode("remote-node", buildNewFakeTransportAddress(), Version.CURRENT);
        discoveryAddress = getDiscoveryAddress();
        remoteClusterName = "local-cluster";
        dropHandshake = true;

        FailureListener failureListener = new FailureListener();
        handshakingTransportAddressConnector.connectToRemoteMasterNode(discoveryAddress, failureListener);
        Thread.sleep(PROBE_HANDSHAKE_TIMEOUT_SETTING.get(Settings.EMPTY).millis());
        failureListener.assertFailure();
    }

    private TransportAddress getDiscoveryAddress() {
        return randomBoolean() ? remoteNode.getAddress() : buildNewFakeTransportAddress();
    }

    private class FailureListener implements ActionListener<DiscoveryNode> {
        final CountDownLatch completionLatch = new CountDownLatch(1);

        @Override
        public void onResponse(DiscoveryNode discoveryNode) {
            fail(discoveryNode.toString());
        }

        @Override
        public void onFailure(Exception e) {
            completionLatch.countDown();
        }

        void assertFailure() throws InterruptedException {
            assertTrue(completionLatch.await(30, TimeUnit.SECONDS));
        }
    }
}
