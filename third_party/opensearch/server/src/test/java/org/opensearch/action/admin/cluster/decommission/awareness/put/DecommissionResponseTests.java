/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.put;

import org.density.test.DensityTestCase;

import java.io.IOException;

public class DecommissionResponseTests extends DensityTestCase {
    public void testSerialization() throws IOException {
        final DecommissionResponse originalRequest = new DecommissionResponse(true);
        copyWriteable(originalRequest, writableRegistry(), DecommissionResponse::new);
        // there are no fields so we're just checking that this doesn't throw anything
    }
}
