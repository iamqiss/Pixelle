/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.snapshots.blobstore;

import org.density.index.snapshots.IndexShardSnapshotStatus;

/**
 * Base interface for shard snapshot status
 *
 * @density.internal
 */
@FunctionalInterface
public interface IndexShardSnapshot {
    IndexShardSnapshotStatus getIndexShardSnapshotStatus();
}
