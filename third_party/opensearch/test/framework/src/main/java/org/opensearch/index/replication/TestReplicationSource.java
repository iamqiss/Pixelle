/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.replication;

import org.density.core.action.ActionListener;
import org.density.index.shard.IndexShard;
import org.density.index.store.StoreFileMetadata;
import org.density.indices.replication.CheckpointInfoResponse;
import org.density.indices.replication.GetSegmentFilesResponse;
import org.density.indices.replication.SegmentReplicationSource;
import org.density.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * This class is used by unit tests implementing SegmentReplicationSource
 */
public abstract class TestReplicationSource implements SegmentReplicationSource {

    @Override
    public abstract void getCheckpointMetadata(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        ActionListener<CheckpointInfoResponse> listener
    );

    @Override
    public abstract void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        IndexShard indexShard,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<GetSegmentFilesResponse> listener
    );

    @Override
    public String getDescription() {
        return "This is a test description";
    }
}
