/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.action.ActionRequestValidationException;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.plugin.wlm.WorkloadManagementTestUtils;
import org.density.test.DensityTestCase;

import java.io.IOException;

public class DeleteWorkloadGroupRequestTests extends DensityTestCase {

    /**
     * Test case to verify the serialization and deserialization of DeleteWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest(WorkloadManagementTestUtils.NAME_ONE);
        assertEquals(WorkloadManagementTestUtils.NAME_ONE, request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        DeleteWorkloadGroupRequest otherRequest = new DeleteWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case to validate a DeleteWorkloadGroupRequest.
     */
    public void testSerializationWithNull() throws IOException {
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest((String) null);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertFalse(actionRequestValidationException.getMessage().isEmpty());
    }
}
