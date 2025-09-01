/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.transport.TransportResponse;
import org.density.index.store.StoreFileMetadata;

import java.io.IOException;
import java.util.List;

/**
 * Response from a {@link SegmentReplicationSource} indicating that a replication event has completed.
 *
 * @density.internal
 */
public class GetSegmentFilesResponse extends TransportResponse {

    List<StoreFileMetadata> files;

    public GetSegmentFilesResponse(List<StoreFileMetadata> files) {
        this.files = files;
    }

    public GetSegmentFilesResponse(StreamInput out) throws IOException {
        out.readList(StoreFileMetadata::new);
    }

    public List<StoreFileMetadata> getFiles() {
        return files;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(files);
    }
}
