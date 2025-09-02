/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.state;

import org.density.action.ActionRequestValidationException;
import org.density.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

import java.io.IOException;

public class UpdateIngestionStateRequestTests extends DensityTestCase {

    public void testConstructor() {
        String[] indices = new String[] { "index1", "index2" };
        int[] shards = new int[] { 0, 1, 2 };
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(indices, shards);
        assertArrayEquals(indices, request.getIndex());
        assertArrayEquals(shards, request.getShards());
        assertNull(request.getIngestionPaused());
    }

    public void testSerialization() throws IOException {
        String[] indices = new String[] { "index1", "index2" };
        int[] shards = new int[] { 0, 1, 2 };
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(indices, shards);
        request.setIngestionPaused(false);
        request.setResetSettings(
            new ResumeIngestionRequest.ResetSettings[] {
                new ResumeIngestionRequest.ResetSettings(0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "0") }
        );

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                UpdateIngestionStateRequest deserializedRequest = new UpdateIngestionStateRequest(in);
                assertArrayEquals(request.getIndex(), deserializedRequest.getIndex());
                assertArrayEquals(request.getShards(), deserializedRequest.getShards());
                assertEquals(request.getResetSettings()[0].getShard(), deserializedRequest.getResetSettings()[0].getShard());
                assertEquals(request.getResetSettings()[0].getMode(), deserializedRequest.getResetSettings()[0].getMode());
                assertEquals(request.getResetSettings()[0].getValue(), deserializedRequest.getResetSettings()[0].getValue());
                assertFalse(deserializedRequest.getIngestionPaused());
            }
        }
    }

    public void testValidation() {
        // Test with null indices
        UpdateIngestionStateRequest request = new UpdateIngestionStateRequest(null, new int[] {});
        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);

        // Test with valid indices
        request = new UpdateIngestionStateRequest(new String[] { "index1" }, new int[] {});
        validationException = request.validate();
        assertNull(validationException);
    }
}
