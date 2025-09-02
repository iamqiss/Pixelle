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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.cluster.routing.allocation;

import org.density.Version;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.DensityAllocationTestCase;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.routing.ShardRoutingState;
import org.density.cluster.routing.allocation.command.AllocationCommands;
import org.density.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.density.common.settings.Settings;

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class TrackFailedAllocationNodesTests extends DensityAllocationTestCase {

    public void testTrackFailedNodes() {
        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        AllocationService allocationService = createAllocationService();
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0))
            .build();
        DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
        for (int i = 0; i < 5; i++) {
            discoNodes.add(newNode("node-" + i));
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(discoNodes)
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("idx")).build())
            .build();
        clusterState = allocationService.reroute(clusterState, "reroute");
        Set<String> failedNodeIds = new HashSet<>();

        // track the failed nodes if shard is not started
        for (int i = 0; i < maxRetries; i++) {
            failedNodeIds.add(clusterState.routingTable().index("idx").shard(0).shards().get(0).currentNodeId());
            clusterState = allocationService.applyFailedShard(
                clusterState,
                clusterState.routingTable().index("idx").shard(0).shards().get(0),
                randomBoolean()
            );
            assertThat(
                clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(),
                equalTo(failedNodeIds)
            );
        }

        // reroute with retryFailed=true should discard the failedNodes
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).state(), equalTo(ShardRoutingState.UNASSIGNED));
        clusterState = allocationService.reroute(clusterState, new AllocationCommands(), false, true).getClusterState();
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(), empty());

        // do not track the failed nodes while shard is started
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).state(), equalTo(ShardRoutingState.STARTED));
        clusterState = allocationService.applyFailedShard(
            clusterState,
            clusterState.routingTable().index("idx").shard(0).shards().get(0),
            false
        );
        assertThat(clusterState.routingTable().index("idx").shard(0).shards().get(0).unassignedInfo().getFailedNodeIds(), empty());
    }
}
