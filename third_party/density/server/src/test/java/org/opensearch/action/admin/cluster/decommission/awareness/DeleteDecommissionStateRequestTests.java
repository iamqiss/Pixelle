/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness;

import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

import java.io.IOException;

public class DeleteDecommissionStateRequestTests extends DensityTestCase {

    public void testSerialization() throws IOException {
        final DeleteDecommissionStateRequest originalRequest = new DeleteDecommissionStateRequest();

        final DeleteDecommissionStateRequest cloneRequest;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                cloneRequest = new DeleteDecommissionStateRequest(in);
            }
        }
        assertEquals(cloneRequest.clusterManagerNodeTimeout(), originalRequest.clusterManagerNodeTimeout());
    }
}
