/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.snapshots;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown if requested snapshot/s can't be deleted
 *
 * @density.internal
 */
public class SnapshotInUseDeletionException extends SnapshotException {

    public SnapshotInUseDeletionException(final String repositoryName, final String snapshotName, final String msg) {
        super(repositoryName, snapshotName, msg);
    }

    public SnapshotInUseDeletionException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }
}
