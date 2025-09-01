/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.state;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class UpdateIngestionStateResponseTests extends DensityTestCase {

    public void testSerialization() throws IOException {
        List<DefaultShardOperationFailedException> shardFailures = Collections.singletonList(
            new DefaultShardOperationFailedException("index1", 0, new Exception("test failure"))
        );
        UpdateIngestionStateResponse response = new UpdateIngestionStateResponse(true, 3, 2, 1, shardFailures);
        response.setErrorMessage("test error");

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                UpdateIngestionStateResponse deserializedResponse = new UpdateIngestionStateResponse(in);
                assertTrue(deserializedResponse.isAcknowledged());
                assertEquals(3, deserializedResponse.getTotalShards());
                assertEquals(2, deserializedResponse.getSuccessfulShards());
                assertEquals(1, deserializedResponse.getFailedShards());
                assertNotNull(deserializedResponse.getShardFailureList());
                assertEquals(1, deserializedResponse.getShardFailureList().length);
                assertEquals("index1", deserializedResponse.getShardFailureList()[0].index());
                assertEquals(0, deserializedResponse.getShardFailureList()[0].shard());
                assertEquals("test error", deserializedResponse.getErrorMessage());
            }
        }
    }
}
