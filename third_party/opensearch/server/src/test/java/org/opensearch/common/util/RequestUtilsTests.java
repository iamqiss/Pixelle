/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.util;

import org.density.core.common.Strings;
import org.density.test.DensityTestCase;

public class RequestUtilsTests extends DensityTestCase {

    public void testGenerateID() {
        assertTrue(Strings.hasText(RequestUtils.generateID()));
    }
}
