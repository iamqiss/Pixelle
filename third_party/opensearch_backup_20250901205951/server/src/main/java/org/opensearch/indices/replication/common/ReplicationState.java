/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.common;

import org.density.common.annotation.PublicApi;

/**
 * Represents a state object used to track copying of segments from an external source
 *
 * @density.api
 */
@PublicApi(since = "2.2.0")
public interface ReplicationState {
    ReplicationLuceneIndex getIndex();

    ReplicationTimer getTimer();
}
