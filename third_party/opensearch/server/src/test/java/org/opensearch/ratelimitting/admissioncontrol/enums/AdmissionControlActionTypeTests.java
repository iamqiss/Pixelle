/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.ratelimitting.admissioncontrol.enums;

import org.density.test.DensityTestCase;

public class AdmissionControlActionTypeTests extends DensityTestCase {

    public void testValidActionType() {
        assertEquals(AdmissionControlActionType.SEARCH.getType(), "search");
        assertEquals(AdmissionControlActionType.INDEXING.getType(), "indexing");
        assertEquals(AdmissionControlActionType.fromName("search"), AdmissionControlActionType.SEARCH);
        assertEquals(AdmissionControlActionType.fromName("indexing"), AdmissionControlActionType.INDEXING);
    }

    public void testInValidActionType() {
        String name = "test";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> AdmissionControlActionType.fromName(name));
        assertEquals(ex.getMessage(), "Not Supported TransportAction Type: " + name);
    }
}
