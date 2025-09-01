/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness;

import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.density.test.DensityTestCase;

import java.io.IOException;

public class DeleteDecommissionStateResponseTests extends DensityTestCase {

    public void testSerialization() throws IOException {
        final DeleteDecommissionStateResponse originalResponse = new DeleteDecommissionStateResponse(true);

        final DeleteDecommissionStateResponse deserialized = copyWriteable(
            originalResponse,
            writableRegistry(),
            DeleteDecommissionStateResponse::new
        );
        assertEquals(deserialized, originalResponse);

    }
}
