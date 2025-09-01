/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule;

import org.density.action.ActionRequest;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNodes;
import org.density.common.settings.Settings;
import org.density.core.action.ActionResponse;
import org.density.plugins.ActionPlugin;
import org.density.rest.RestHandler;
import org.density.rule.action.GetRuleAction;
import org.density.rule.rest.RestCreateRuleAction;
import org.density.rule.rest.RestDeleteRuleAction;
import org.density.rule.rest.RestGetRuleAction;
import org.density.rule.rest.RestUpdateRuleAction;
import org.density.test.DensityTestCase;

import java.util.List;

import static org.mockito.Mockito.mock;

public class RuleFrameworkPluginTests extends DensityTestCase {
    RuleFrameworkPlugin plugin = new RuleFrameworkPlugin();;

    public void testGetActions() {
        List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> handlers = plugin.getActions();
        assertEquals(4, handlers.size());
        assertEquals(GetRuleAction.INSTANCE.name(), handlers.get(0).getAction().name());
    }

    public void testGetRestHandlers() {
        Settings settings = Settings.EMPTY;
        List<RestHandler> handlers = plugin.getRestHandlers(
            settings,
            mock(org.density.rest.RestController.class),
            null,
            null,
            null,
            mock(IndexNameExpressionResolver.class),
            () -> mock(DiscoveryNodes.class)
        );

        assertTrue(handlers.get(0) instanceof RestGetRuleAction);
        assertTrue(handlers.get(1) instanceof RestDeleteRuleAction);
        assertTrue(handlers.get(2) instanceof RestCreateRuleAction);
        assertTrue(handlers.get(3) instanceof RestUpdateRuleAction);
    }
}
