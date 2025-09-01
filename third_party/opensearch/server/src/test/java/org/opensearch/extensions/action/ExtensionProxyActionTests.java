/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.action;

import org.density.test.DensityTestCase;

public class ExtensionProxyActionTests extends DensityTestCase {
    public void testExtensionProxyAction() {
        assertEquals("cluster:internal/extensions", ExtensionProxyAction.NAME);
        assertEquals(ExtensionProxyAction.class, ExtensionProxyAction.INSTANCE.getClass());
    }
}
