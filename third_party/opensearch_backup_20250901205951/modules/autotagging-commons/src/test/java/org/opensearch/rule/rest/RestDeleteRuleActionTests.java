/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.rest;

import org.density.test.DensityTestCase;

public class RestDeleteRuleActionTests extends DensityTestCase {
    RestDeleteRuleAction action = new RestDeleteRuleAction();

    public void testGetName() {
        assertEquals("delete_rule", action.getName());
    }

    public void testRoutes() {
        var routes = action.routes();
        assertEquals(1, routes.size());
        assertTrue(routes.stream().anyMatch(r -> r.getMethod().name().equals("DELETE") && r.getPath().equals("_rules/{featureType}/{id}")));
    }
}
