/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.allocation;

import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.routing.IndexShardRoutingTable;
import org.density.cluster.routing.ShardRouting;
import org.density.common.settings.Settings;
import org.density.indices.replication.common.ReplicationType;
import org.density.remotestore.RemoteStoreBaseIntegTestCase;
import org.density.test.DensityIntegTestCase;

import java.util.List;
import java.util.stream.Collectors;

import static org.density.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchReplicaAllocationIT extends RemoteStoreBaseIntegTestCase {

    public void testSearchReplicaAllocatedToDedicatedSearchNode() {
        internalCluster().startClusterManagerOnlyNode();
        String primaryNode = internalCluster().startDataOnlyNode();
        internalCluster().startSearchOnlyNode();

        assertEquals(3, cluster().size());

        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureGreen("test");
        // ensure primary is not on searchNode
        IndexShardRoutingTable routingTable = getRoutingTable();
        assertEquals(primaryNode, getNodeName(routingTable.primaryShard().currentNodeId()));
    }

    public void testSearchReplicaDedicatedIncludes_DoNotAssignToOtherNodes() {
        internalCluster().startNodes(2);
        final String node_1 = internalCluster().startSearchOnlyNode();
        assertEquals(3, cluster().size());

        logger.info("--> creating an index with no replicas");
        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 2)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureYellowAndNoInitializingShards("test");
        IndexShardRoutingTable routingTable = getRoutingTable();
        assertEquals(2, routingTable.searchOnlyReplicas().size());
        List<ShardRouting> assignedSearchShards = routingTable.searchOnlyReplicas()
            .stream()
            .filter(ShardRouting::assignedToNode)
            .collect(Collectors.toList());
        assertEquals(1, assignedSearchShards.size());
        assertEquals(node_1, getNodeName(assignedSearchShards.get(0).currentNodeId()));
        assertEquals(1, routingTable.searchOnlyReplicas().stream().filter(ShardRouting::unassigned).count());
    }

    public void testSearchReplicaDedicatedIncludes_WhenNotSetDoNotAssign() {
        internalCluster().startNodes(2);
        assertEquals(2, cluster().size());

        createIndex(
            "test",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        ensureYellowAndNoInitializingShards("test");
        IndexShardRoutingTable routingTable = getRoutingTable();
        assertNull(routingTable.searchOnlyReplicas().get(0).currentNodeId());

        // Add a search node
        final String searchNode = internalCluster().startSearchOnlyNode();

        ensureGreen("test");
        assertEquals(searchNode, getNodeName(getRoutingTable().searchOnlyReplicas().get(0).currentNodeId()));
    }

    private IndexShardRoutingTable getRoutingTable() {
        return getClusterState().routingTable().index("test").getShards().get(0);
    }

    private String getNodeName(String id) {
        return getClusterState().nodes().get(id).getName();
    }
}
