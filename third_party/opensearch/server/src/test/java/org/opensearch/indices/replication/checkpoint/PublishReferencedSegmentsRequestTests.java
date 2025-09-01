/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.util.set.Sets;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.test.DensityTestCase;

import java.util.Collections;

import static org.hamcrest.core.IsEqual.equalTo;

public class PublishReferencedSegmentsRequestTests extends DensityTestCase {

    public void testPublishReferencedSegmentsRequest() {
        ReferencedSegmentsCheckpoint checkpoint = new ReferencedSegmentsCheckpoint(
            new ShardId(new Index("1", "1"), 0),
            0,
            1,
            0,
            "",
            Collections.emptyMap(),
            Sets.newHashSet("_1", "_2", "_3")
        );
        PublishReferencedSegmentsRequest request = new PublishReferencedSegmentsRequest(checkpoint);
        assertNull(request.validate());
        assertEquals(checkpoint, request.getReferencedSegmentsCheckpoint());
    }

    public void testSerialize() throws Exception {
        ReferencedSegmentsCheckpoint checkpoint = new ReferencedSegmentsCheckpoint(
            new ShardId(new Index("1", "1"), 0),
            0,
            1,
            0,
            "",
            Collections.emptyMap(),
            Sets.newHashSet("_1", "_2", "_3")
        );
        PublishReferencedSegmentsRequest originalRequest = new PublishReferencedSegmentsRequest(checkpoint);
        PublishReferencedSegmentsRequest cloneRequest;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                cloneRequest = new PublishReferencedSegmentsRequest(in);
            }
        }
        assertThat(cloneRequest, equalTo(originalRequest));
        assertThat(cloneRequest.getReferencedSegmentsCheckpoint(), equalTo(originalRequest.getReferencedSegmentsCheckpoint()));
    }
}
