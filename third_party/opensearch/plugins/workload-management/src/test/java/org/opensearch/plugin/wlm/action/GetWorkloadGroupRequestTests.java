/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.plugin.wlm.WorkloadManagementTestUtils;
import org.density.test.DensityTestCase;

import java.io.IOException;

public class GetWorkloadGroupRequestTests extends DensityTestCase {

    /**
     * Test case to verify the serialization and deserialization of GetWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        GetWorkloadGroupRequest request = new GetWorkloadGroupRequest(WorkloadManagementTestUtils.NAME_ONE);
        assertEquals(WorkloadManagementTestUtils.NAME_ONE, request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetWorkloadGroupRequest otherRequest = new GetWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case to verify the serialization and deserialization of GetWorkloadGroupRequest when name is null.
     */
    public void testSerializationWithNull() throws IOException {
        GetWorkloadGroupRequest request = new GetWorkloadGroupRequest((String) null);
        assertNull(request.getName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        GetWorkloadGroupRequest otherRequest = new GetWorkloadGroupRequest(streamInput);
        assertEquals(request.getName(), otherRequest.getName());
    }

    /**
     * Test case the validation function of GetWorkloadGroupRequest
     */
    public void testValidation() {
        GetWorkloadGroupRequest request = new GetWorkloadGroupRequest("a".repeat(51));
        assertThrows(IllegalArgumentException.class, request::validate);
    }
}
