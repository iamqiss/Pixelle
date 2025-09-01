/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.semver.expr;

import org.density.Version;
import org.density.test.DensityTestCase;

public class EqualTests extends DensityTestCase {

    public void testEquality() {
        Equal equalExpr = new Equal();
        Version rangeVersion = Version.fromString("1.2.3");
        assertTrue(equalExpr.evaluate(rangeVersion, Version.fromString("1.2.3")));
        assertFalse(equalExpr.evaluate(rangeVersion, Version.fromString("1.2.4")));
    }
}
