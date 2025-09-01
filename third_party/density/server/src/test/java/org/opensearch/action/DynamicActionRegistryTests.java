/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action;

import org.density.action.ActionModule.DynamicActionRegistry;
import org.density.action.main.MainAction;
import org.density.action.support.ActionFilters;
import org.density.action.support.TransportAction;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.Writeable;
import org.density.extensions.action.ExtensionAction;
import org.density.extensions.action.ExtensionTransportAction;
import org.density.extensions.rest.RestSendToExtensionAction;
import org.density.rest.NamedRoute;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.tasks.Task;
import org.density.tasks.TaskManager;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class DynamicActionRegistryTests extends DensityTestCase {

    public void testDynamicActionRegistry() {
        ActionFilters emptyFilters = new ActionFilters(Collections.emptySet());
        Map<ActionType, TransportAction> testMap = Map.of(TestAction.INSTANCE, new TestTransportAction("test-action", emptyFilters, null));

        DynamicActionRegistry dynamicActionRegistry = new DynamicActionRegistry();
        dynamicActionRegistry.registerUnmodifiableActionMap(testMap);

        // Should contain the immutable map entry
        assertNotNull(dynamicActionRegistry.get(TestAction.INSTANCE));
        // Should not contain anything not added
        assertNull(dynamicActionRegistry.get(MainAction.INSTANCE));

        // ExtensionsAction not yet registered
        ExtensionAction testExtensionAction = new ExtensionAction("extensionId", "actionName");
        ExtensionTransportAction testExtensionTransportAction = new ExtensionTransportAction("test-action", emptyFilters, null, null);
        assertNull(dynamicActionRegistry.get(testExtensionAction));

        // Register an extension action
        // Should insert without problem
        try {
            dynamicActionRegistry.registerDynamicAction(testExtensionAction, testExtensionTransportAction);
        } catch (Exception e) {
            fail("Should not have thrown exception registering action: " + e);
        }
        assertEquals(testExtensionTransportAction, dynamicActionRegistry.get(testExtensionAction));

        // Should fail inserting twice
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> dynamicActionRegistry.registerDynamicAction(testExtensionAction, testExtensionTransportAction)
        );
        assertEquals("action [actionName] already registered", ex.getMessage());
        // Should remove without problem
        try {
            dynamicActionRegistry.unregisterDynamicAction(testExtensionAction);
        } catch (Exception e) {
            fail("Should not have thrown exception unregistering action: " + e);
        }
        // Should have been removed
        assertNull(dynamicActionRegistry.get(testExtensionAction));

        // Should fail removing twice
        ex = assertThrows(IllegalArgumentException.class, () -> dynamicActionRegistry.unregisterDynamicAction(testExtensionAction));
        assertEquals("action [actionName] was not registered", ex.getMessage());
    }

    public void testDynamicActionRegistryWithNamedRoutes() {
        RestSendToExtensionAction action = mock(RestSendToExtensionAction.class);
        RestSendToExtensionAction action2 = mock(RestSendToExtensionAction.class);
        NamedRoute r1 = new NamedRoute.Builder().method(RestRequest.Method.GET).path("/foo").uniqueName("foo").build();
        NamedRoute r2 = new NamedRoute.Builder().method(RestRequest.Method.PUT).path("/bar").uniqueName("bar").build();
        RestHandler.Route r3 = new RestHandler.Route(RestRequest.Method.DELETE, "/foo");

        DynamicActionRegistry registry = new DynamicActionRegistry();
        registry.registerDynamicRoute(r1, action);
        registry.registerDynamicRoute(r2, action2);

        assertTrue(registry.isActionRegistered("foo"));
        assertEquals(action, registry.get(r1));
        assertTrue(registry.isActionRegistered("bar"));
        assertEquals(action2, registry.get(r2));
        assertNull(registry.get(r3));

        registry.unregisterDynamicRoute(r2);

        assertTrue(registry.isActionRegistered("foo"));
        assertFalse(registry.isActionRegistered("bar"));
    }

    public void testDynamicActionRegistryWithNamedRoutesAndLegacyActionNames() {
        RestSendToExtensionAction action = mock(RestSendToExtensionAction.class);
        RestSendToExtensionAction action2 = mock(RestSendToExtensionAction.class);
        NamedRoute r1 = new NamedRoute.Builder().method(RestRequest.Method.GET)
            .path("/foo")
            .uniqueName("foo")
            .legacyActionNames(Set.of("cluster:admin/density/abc/foo"))
            .build();
        NamedRoute r2 = new NamedRoute.Builder().method(RestRequest.Method.PUT)
            .path("/bar")
            .uniqueName("bar")
            .legacyActionNames(Set.of("cluster:admin/density/xyz/bar"))
            .build();

        DynamicActionRegistry registry = new DynamicActionRegistry();
        registry.registerDynamicRoute(r1, action);
        registry.registerDynamicRoute(r2, action2);

        assertTrue(registry.isActionRegistered("cluster:admin/density/abc/foo"));
        assertTrue(registry.isActionRegistered("cluster:admin/density/xyz/bar"));

        registry.unregisterDynamicRoute(r2);

        assertTrue(registry.isActionRegistered("cluster:admin/density/abc/foo"));
        assertFalse(registry.isActionRegistered("cluster:admin/density/xyz/bar"));
    }

    private static final class TestAction extends ActionType<ActionResponse> {
        public static final TestAction INSTANCE = new TestAction();

        private TestAction() {
            super("test-action", new Writeable.Reader<ActionResponse>() {
                @Override
                public ActionResponse read(StreamInput in) throws IOException {
                    return null;
                }
            });
        }
    };

    private static final class TestTransportAction extends TransportAction<ActionRequest, ActionResponse> {
        protected TestTransportAction(String actionName, ActionFilters actionFilters, TaskManager taskManager) {
            super(actionName, actionFilters, taskManager);
        }

        @Override
        protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {}
    }
}
