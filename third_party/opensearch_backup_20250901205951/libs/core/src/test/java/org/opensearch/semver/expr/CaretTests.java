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

public class CaretTests extends DensityTestCase {

    public void testMinorAndPatchVersionVariability() {
        Caret caretExpr = new Caret();
        Version rangeVersion = Version.fromString("1.2.3");

        // Compatible versions
        assertTrue(caretExpr.evaluate(rangeVersion, Version.fromString("1.2.3")));
        assertTrue(caretExpr.evaluate(rangeVersion, Version.fromString("1.2.4")));
        assertTrue(caretExpr.evaluate(rangeVersion, Version.fromString("1.3.3")));
        assertTrue(caretExpr.evaluate(rangeVersion, Version.fromString("1.9.9")));

        // Incompatible versions
        assertFalse(caretExpr.evaluate(rangeVersion, Version.fromString("1.2.2")));
        assertFalse(caretExpr.evaluate(rangeVersion, Version.fromString("2.0.0")));
    }
}
