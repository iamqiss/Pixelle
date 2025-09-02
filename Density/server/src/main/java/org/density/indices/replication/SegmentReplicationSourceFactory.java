/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication;

import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.service.ClusterService;
import org.density.core.index.shard.ShardId;
import org.density.index.shard.IndexShard;
import org.density.indices.recovery.RecoverySettings;
import org.density.transport.TransportService;

/**
 * Factory to build {@link SegmentReplicationSource} used by {@link SegmentReplicationTargetService}.
 *
 * @density.internal
 */
public class SegmentReplicationSourceFactory {

    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    public SegmentReplicationSourceFactory(
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService
    ) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
    }

    public SegmentReplicationSource get(IndexShard shard) {
        if (shard.indexSettings().isAssignedOnRemoteNode()) {
            return new RemoteStoreReplicationSource(shard);
        } else {
            return new PrimaryShardReplicationSource(
                shard.recoveryState().getTargetNode(),
                shard.routingEntry().allocationId().getId(),
                transportService,
                recoverySettings,
                getPrimaryNode(shard.shardId())
            );
        }
    }

    private DiscoveryNode getPrimaryNode(ShardId shardId) {
        ShardRouting primaryShard = clusterService.state().routingTable().shardRoutingTable(shardId).primaryShard();
        DiscoveryNode node = clusterService.state().nodes().get(primaryShard.currentNodeId());
        if (node == null) {
            throw new IllegalStateException("Cannot replicate, primary shard for " + shardId + " is not allocated on any node");
        }
        return node;
    }
}
