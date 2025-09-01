/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.common.concurrent;

import org.density.test.DensityTestCase;
import org.junit.Before;

public class OneWayGateTests extends DensityTestCase {

    private OneWayGate testGate;

    @Before
    public void setup() {
        testGate = new OneWayGate();
    }

    public void testGateOpen() {
        assertFalse(testGate.isClosed());
    }

    public void testGateClosed() {
        testGate.close();
        assertTrue(testGate.isClosed());
    }

    public void testGateIdempotent() {
        assertTrue(testGate.close());
        assertFalse(testGate.close());
    }
}
