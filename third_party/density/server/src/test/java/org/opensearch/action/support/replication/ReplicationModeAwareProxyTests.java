/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.support.replication;

import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.AllocationId;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.routing.ShardRoutingState;
import org.density.cluster.routing.TestShardRouting;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.index.shard.IndexShardTestUtils;
import org.density.test.DensityTestCase;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ReplicationModeAwareProxyTests extends DensityTestCase {

    /*
    Replication action running on the same primary copy from which it originates.
    Action should not run and proxy should return ReplicationMode.NO_REPLICATION
     */
    public void testDetermineReplicationModeTargetRoutingCurrentPrimary() {
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            null,
            true,
            ShardRoutingState.STARTED,
            AllocationId.newInitializing("abc")
        );
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            null,
            true,
            ShardRoutingState.STARTED,
            AllocationId.newInitializing("abc")
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder().add(IndexShardTestUtils.getFakeRemoteEnabledNode("dummy-node")).build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            randomBoolean()
        );
        assertEquals(ReplicationMode.NO_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from failing primary to replica being promoted to primary
     Action should run and proxy should return ReplicationMode.FULL_REPLICATION
     */
    public void testDetermineReplicationModeTargetRoutingRelocatingPrimary() {
        AllocationId primaryId = AllocationId.newRelocation(AllocationId.newInitializing());
        AllocationId relocationTargetId = AllocationId.newTargetRelocation(primaryId);
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            null,
            true,
            ShardRoutingState.INITIALIZING,
            relocationTargetId
        );
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            "dummy-node-2",
            true,
            ShardRoutingState.RELOCATING,
            primaryId
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(targetRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            randomBoolean()
        );
        assertEquals(ReplicationMode.FULL_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from remote enabled primary to docrep replica during remote store migration
     Action should run and proxy should return ReplicationMode.FULL_REPLICATION
     */
    public void testDetermineReplicationModeTargetRoutingDocrepShard() {
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            false,
            ShardRoutingState.STARTED
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeDiscoNode(targetRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            false
        );
        assertEquals(ReplicationMode.FULL_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from remote enabled primary to remote replica during remote store migration
     Action should not run and proxy should return ReplicationMode.NO_REPLICATION
     */
    public void testDetermineReplicationModeTargetRoutingRemoteShard() {
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            false,
            ShardRoutingState.STARTED
        );
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            true,
            ShardRoutingState.STARTED
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(targetRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            false
        );
        assertEquals(ReplicationMode.NO_REPLICATION, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from remote enabled primary to remote enabled replica during remote store migration
     with an explicit replication mode specified
     Action should run and proxy should return the overridden Replication Mode
     */
    public void testDetermineReplicationWithExplicitOverrideTargetRoutingRemoteShard() {
        ReplicationMode replicationModeOverride = ReplicationMode.PRIMARY_TERM_VALIDATION;
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            false,
            ShardRoutingState.STARTED
        );
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            true,
            ShardRoutingState.STARTED
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            replicationModeOverride,
            DiscoveryNodes.builder()
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(targetRouting.currentNodeId()))
                .add(IndexShardTestUtils.getFakeRemoteEnabledNode(primaryRouting.currentNodeId()))
                .build(),
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            false
        );
        assertEquals(replicationModeOverride, replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting));
    }

    /*
     Replication action originating from remote enabled primary with remote enabled index settings enabled
     Action should not query the DiscoveryNodes object
     */
    public void testDetermineReplicationWithRemoteIndexSettingsEnabled() {
        DiscoveryNodes mockDiscoveryNodes = mock(DiscoveryNodes.class);
        ShardRouting primaryRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node",
            false,
            ShardRoutingState.STARTED
        );
        ShardRouting targetRouting = TestShardRouting.newShardRouting(
            new ShardId(new Index("test_index", "_na_"), 0),
            "dummy-node-2",
            true,
            ShardRoutingState.STARTED
        );
        final ReplicationModeAwareProxy replicationModeAwareProxy = new ReplicationModeAwareProxy(
            ReplicationMode.NO_REPLICATION,
            mockDiscoveryNodes,
            mock(TransportReplicationAction.ReplicasProxy.class),
            mock(TransportReplicationAction.ReplicasProxy.class),
            true
        );
        replicationModeAwareProxy.determineReplicationMode(targetRouting, primaryRouting);
        // Verify no interactions with the DiscoveryNodes object
        verify(mockDiscoveryNodes, never()).get(anyString());
    }
}
