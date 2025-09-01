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

package org.density.action.admin.cluster.state;

import org.density.cluster.ClusterState;
import org.density.cluster.coordination.ClusterBootstrapService;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.discovery.ClusterManagerNotDiscoveredException;
import org.density.plugins.Plugin;
import org.density.test.DensityIntegTestCase;
import org.density.test.transport.MockTransportService;
import org.density.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.density.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@DensityIntegTestCase.ClusterScope(numDataNodes = 0, scope = DensityIntegTestCase.Scope.TEST)
public class TransportClusterStateActionDisruptionIT extends DensityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testNonLocalRequestAlwaysFindsClusterManager() throws Exception {
        runRepeatedlyWhileChangingClusterManager(() -> {
            final ClusterStateRequestBuilder clusterStateRequestBuilder = client().admin()
                .cluster()
                .prepareState()
                .clear()
                .setNodes(true)
                .setClusterManagerNodeTimeout("100ms");
            final ClusterStateResponse clusterStateResponse;
            try {
                clusterStateResponse = clusterStateRequestBuilder.get();
            } catch (ClusterManagerNotDiscoveredException e) {
                return; // ok, we hit the disconnected node
            }
            assertNotNull(
                "should always contain a cluster-manager node",
                clusterStateResponse.getState().nodes().getClusterManagerNodeId()
            );
        });
    }

    public void testLocalRequestAlwaysSucceeds() throws Exception {
        runRepeatedlyWhileChangingClusterManager(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final DiscoveryNodes discoveryNodes = client(node).admin()
                .cluster()
                .prepareState()
                .clear()
                .setLocal(true)
                .setNodes(true)
                .setClusterManagerNodeTimeout("100ms")
                .get()
                .getState()
                .nodes();
            for (DiscoveryNode discoveryNode : discoveryNodes) {
                if (discoveryNode.getName().equals(node)) {
                    return;
                }
            }
            fail("nodes did not contain [" + node + "]: " + discoveryNodes);
        });
    }

    public void testNonLocalRequestAlwaysFindsClusterManagerAndWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingClusterManager(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final long metadataVersion = internalCluster().getInstance(ClusterService.class, node)
                .getClusterApplierService()
                .state()
                .metadata()
                .version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            final ClusterStateRequestBuilder clusterStateRequestBuilder = client(node).admin()
                .cluster()
                .prepareState()
                .clear()
                .setNodes(true)
                .setMetadata(true)
                .setClusterManagerNodeTimeout(TimeValue.timeValueMillis(100))
                .setWaitForTimeOut(TimeValue.timeValueMillis(100))
                .setWaitForMetadataVersion(waitForMetadataVersion);
            final ClusterStateResponse clusterStateResponse;
            try {
                clusterStateResponse = clusterStateRequestBuilder.get();
            } catch (ClusterManagerNotDiscoveredException e) {
                return; // ok, we hit the disconnected node
            }
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final ClusterState state = clusterStateResponse.getState();
                assertNotNull("should always contain a cluster-manager node", state.nodes().getClusterManagerNodeId());
                assertThat("waited for metadata version", state.metadata().version(), greaterThanOrEqualTo(waitForMetadataVersion));
            }
        });
    }

    public void testLocalRequestWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingClusterManager(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final long metadataVersion = internalCluster().getInstance(ClusterService.class, node)
                .getClusterApplierService()
                .state()
                .metadata()
                .version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            final ClusterStateResponse clusterStateResponse = client(node).admin()
                .cluster()
                .prepareState()
                .clear()
                .setLocal(true)
                .setMetadata(true)
                .setWaitForMetadataVersion(waitForMetadataVersion)
                .setClusterManagerNodeTimeout(TimeValue.timeValueMillis(100))
                .setWaitForTimeOut(TimeValue.timeValueMillis(100))
                .get();
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final Metadata metadata = clusterStateResponse.getState().metadata();
                assertThat(
                    "waited for metadata version " + waitForMetadataVersion + " with node " + node,
                    metadata.version(),
                    greaterThanOrEqualTo(waitForMetadataVersion)
                );
            }
        });
    }

    public void runRepeatedlyWhileChangingClusterManager(Runnable runnable) throws Exception {
        internalCluster().startNodes(3);

        assertBusy(
            () -> assertThat(
                client().admin()
                    .cluster()
                    .prepareState()
                    .clear()
                    .setMetadata(true)
                    .get()
                    .getState()
                    .getLastCommittedConfiguration()
                    .getNodeIds()
                    .stream()
                    .filter(n -> ClusterBootstrapService.isBootstrapPlaceholder(n) == false)
                    .collect(Collectors.toSet()),
                hasSize(3)
            )
        );

        final String clusterManagerName = internalCluster().getClusterManagerName();

        final AtomicBoolean shutdown = new AtomicBoolean();
        final Thread assertingThread = new Thread(() -> {
            while (shutdown.get() == false) {
                runnable.run();
            }
        }, "asserting thread");

        final Thread updatingThread = new Thread(() -> {
            String value = "none";
            while (shutdown.get() == false) {
                value = "none".equals(value) ? "all" : "none";
                final String nonClusterManagerNode = randomValueOtherThan(
                    clusterManagerName,
                    () -> randomFrom(internalCluster().getNodeNames())
                );
                assertAcked(
                    client(nonClusterManagerNode).admin()
                        .cluster()
                        .prepareUpdateSettings()
                        .setPersistentSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), value))
                );
            }
        }, "updating thread");

        final List<MockTransportService> mockTransportServices = StreamSupport.stream(
            internalCluster().getInstances(TransportService.class).spliterator(),
            false
        ).map(ts -> (MockTransportService) ts).collect(Collectors.toList());

        assertingThread.start();
        updatingThread.start();

        final MockTransportService clusterManagerTransportService = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            clusterManagerName
        );

        for (MockTransportService mockTransportService : mockTransportServices) {
            if (clusterManagerTransportService != mockTransportService) {
                clusterManagerTransportService.addFailToSendNoConnectRule(mockTransportService);
                mockTransportService.addFailToSendNoConnectRule(clusterManagerTransportService);
            }
        }

        assertBusy(() -> {
            final String nonClusterManagerNode = randomValueOtherThan(
                clusterManagerName,
                () -> randomFrom(internalCluster().getNodeNames())
            );
            final String claimedClusterManagerName = internalCluster().getClusterManagerName(nonClusterManagerNode);
            assertThat(claimedClusterManagerName, not(equalTo(clusterManagerName)));
        });

        shutdown.set(true);
        assertingThread.join();
        updatingThread.join();
        internalCluster().close();
    }

}
