/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.recovery;

import org.density.index.shard.IndexShard;

/**
 * Factory that supplies {@link RecoverySourceHandler}.
 *
 * @density.internal
 */
public class RecoverySourceHandlerFactory {

    public static RecoverySourceHandler create(
        IndexShard shard,
        RecoveryTargetHandler recoveryTarget,
        StartRecoveryRequest request,
        RecoverySettings recoverySettings
    ) {
        boolean isReplicaRecoveryWithRemoteTranslog = request.isPrimaryRelocation() == false && request.targetNode().isRemoteStoreNode();
        if (isReplicaRecoveryWithRemoteTranslog) {
            return new RemoteStorePeerRecoverySourceHandler(
                shard,
                recoveryTarget,
                shard.getThreadPool(),
                request,
                Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                recoverySettings.getMaxConcurrentFileChunks(),
                recoverySettings.getMaxConcurrentOperations()
            );
        } else {
            return new LocalStorePeerRecoverySourceHandler(
                shard,
                recoveryTarget,
                shard.getThreadPool(),
                request,
                Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                recoverySettings.getMaxConcurrentFileChunks(),
                recoverySettings.getMaxConcurrentOperations()
            );
        }
    }
}
