/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.common;

/**
 * Interface for listeners that run when there's a change in {@link ReplicationState}
 *
 * @density.internal
 */
public interface ReplicationListener {

    void onDone(ReplicationState state);

    void onFailure(ReplicationState state, ReplicationFailedException e, boolean sendShardFailure);
}
