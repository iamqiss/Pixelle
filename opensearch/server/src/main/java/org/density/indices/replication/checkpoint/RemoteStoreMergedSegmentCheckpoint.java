/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.density.common.annotation.ExperimentalApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Represents a remote store merged segment checkpoint.
 *
 * @density.internal
 */
@ExperimentalApi
public class RemoteStoreMergedSegmentCheckpoint extends MergedSegmentCheckpoint {
    private final Map<String, String> localToRemoteSegmentFilenameMap;

    public RemoteStoreMergedSegmentCheckpoint(
        MergedSegmentCheckpoint mergedSegmentCheckpoint,
        Map<String, String> localToRemoteSegmentFilenameMap
    ) {
        super(
            mergedSegmentCheckpoint.getShardId(),
            mergedSegmentCheckpoint.getPrimaryTerm(),
            mergedSegmentCheckpoint.getSegmentInfosVersion(),
            mergedSegmentCheckpoint.getLength(),
            mergedSegmentCheckpoint.getCodec(),
            mergedSegmentCheckpoint.getMetadataMap(),
            mergedSegmentCheckpoint.getSegmentName()
        );
        this.localToRemoteSegmentFilenameMap = Collections.unmodifiableMap(localToRemoteSegmentFilenameMap);
    }

    public RemoteStoreMergedSegmentCheckpoint(StreamInput in) throws IOException {
        super(in);
        this.localToRemoteSegmentFilenameMap = Collections.unmodifiableMap(in.readMap(StreamInput::readString, StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(getLocalToRemoteSegmentFilenameMap(), StreamOutput::writeString, StreamOutput::writeString);
    }

    public Map<String, String> getLocalToRemoteSegmentFilenameMap() {
        return this.localToRemoteSegmentFilenameMap;
    }
}
