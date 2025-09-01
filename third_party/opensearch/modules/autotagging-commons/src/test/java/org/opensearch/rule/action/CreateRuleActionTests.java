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

public class CreateRuleActionTests extends DensityTestCase {
    public void testGetName() {
        assertEquals("cluster:admin/density/rule/_create", CreateRuleAction.NAME);
    }

    public void testCreateResponseReader() {
        assertTrue(CreateRuleAction.INSTANCE.getResponseReader() instanceof Writeable.Reader);
    }
}
