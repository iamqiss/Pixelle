/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.core.common.io.stream.Writeable;
import org.density.test.DensityTestCase;

public class GetRuleActionTests extends DensityTestCase {
    public void testGetName() {
        assertEquals("cluster:admin/density/rule/_get", GetRuleAction.NAME);
    }

    public void testGetResponseReader() {
        assertTrue(GetRuleAction.INSTANCE.getResponseReader() instanceof Writeable.Reader);
    }
}
