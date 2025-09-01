/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.recovery;

import org.density.cluster.routing.ShardRouting;
import org.density.common.annotation.PublicApi;
import org.density.indices.cluster.IndicesClusterStateService;
import org.density.indices.replication.common.ReplicationFailedException;
import org.density.indices.replication.common.ReplicationListener;
import org.density.indices.replication.common.ReplicationState;

/**
 * Listener that runs on changes in Recovery state
 *
 * @density.internal
 */
@PublicApi(since = "2.2.0")
public class RecoveryListener implements ReplicationListener {

    /**
     * ShardRouting with which the shard was created
     */
    private final ShardRouting shardRouting;

    /**
     * Primary term with which the shard was created
     */
    private final long primaryTerm;

    private final IndicesClusterStateService indicesClusterStateService;

    public RecoveryListener(
        final ShardRouting shardRouting,
        final long primaryTerm,
        IndicesClusterStateService indicesClusterStateService
    ) {
        this.shardRouting = shardRouting;
        this.primaryTerm = primaryTerm;
        this.indicesClusterStateService = indicesClusterStateService;
    }

    @Override
    public void onDone(ReplicationState state) {
        indicesClusterStateService.handleRecoveryDone(state, shardRouting, primaryTerm);
    }

    @Override
    public void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
        indicesClusterStateService.handleRecoveryFailure(shardRouting, sendShardFailure, e);
    }
}
