/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.rest;

import org.density.Version;
import org.density.action.ActionModule;
import org.density.action.ActionModule.DynamicActionRegistry;
import org.density.action.admin.cluster.health.ClusterHealthAction;
import org.density.action.admin.cluster.health.TransportClusterHealthAction;
import org.density.action.support.ActionFilters;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.network.NetworkService;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsModule;
import org.density.common.util.PageCacheRecycler;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.transport.TransportAddress;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.extensions.DiscoveryExtensionNode;
import org.density.extensions.ExtensionsManager;
import org.density.extensions.action.ExtensionAction;
import org.density.extensions.action.ExtensionTransportAction;
import org.density.identity.IdentityService;
import org.density.rest.NamedRoute;
import org.density.rest.RestHandler.Route;
import org.density.rest.RestRequest.Method;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.transport.MockTransportService;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.nio.MockNioTransport;
import org.density.usage.UsageService;
import org.junit.After;
import org.junit.Before;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;

public class RestSendToExtensionActionTests extends DensityTestCase {

    private TransportService transportService;
    private MockNioTransport transport;
    private DiscoveryExtensionNode discoveryExtensionNode;
    private ActionModule actionModule;
    private DynamicActionRegistry dynamicActionRegistry;
    private IdentityService identityService;
    private final ThreadPool threadPool = new TestThreadPool(RestSendToExtensionActionTests.class.getSimpleName());

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put("cluster.name", "test").build();
        transport = new MockNioTransport(
            settings,
            Version.CURRENT,
            threadPool,
            new NetworkService(Collections.emptyList()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(Collections.emptyList()),
            new NoneCircuitBreakerService(),
            NoopTracer.INSTANCE
        );
        transportService = new MockTransportService(
            settings,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            (boundAddress) -> new DiscoveryNode(
                "test_node",
                "test_node",
                boundAddress.publishAddress(),
                emptyMap(),
                emptySet(),
                Version.CURRENT
            ),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        discoveryExtensionNode = new DiscoveryExtensionNode(
            "firstExtension",
            "uniqueid1",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
            new HashMap<String, String>(),
            Version.fromString("3.0.0"),
            Version.fromString("3.0.0"),
            Collections.emptyList()
        );
        SettingsModule settingsModule = new SettingsModule(settings);
        UsageService usageService = new UsageService();
        actionModule = new ActionModule(
            settingsModule.getSettings(),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            settingsModule.getIndexScopedSettings(),
            settingsModule.getClusterSettings(),
            settingsModule.getSettingsFilter(),
            mock(ThreadPool.class),
            emptyList(),
            null,
            null,
            usageService,
            null,
            new IdentityService(Settings.EMPTY, mock(ThreadPool.class), new ArrayList<>()),
            new ExtensionsManager(Set.of(), new IdentityService(Settings.EMPTY, mock(ThreadPool.class), List.of()))
        );
        identityService = new IdentityService(Settings.EMPTY, mock(ThreadPool.class), new ArrayList<>());
        dynamicActionRegistry = actionModule.getDynamicActionRegistry();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRestSendToExtensionAction() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo foo", "PUT /bar bar", "POST /baz baz"),
            List.of("GET /deprecated/foo foo_deprecated", "Its deprecated")
        );
        RestSendToExtensionAction restSendToExtensionAction = new RestSendToExtensionAction(
            registerRestActionRequest,
            discoveryExtensionNode,
            transportService,
            dynamicActionRegistry,
            identityService
        );

        assertEquals("uniqueid1:send_to_extension_action", restSendToExtensionAction.getName());
        List<Route> expected = new ArrayList<>();
        String uriPrefix = "/_extensions/_uniqueid1";
        expected.add(new Route(Method.GET, uriPrefix + "/foo"));
        expected.add(new Route(Method.PUT, uriPrefix + "/bar"));
        expected.add(new Route(Method.POST, uriPrefix + "/baz"));

        List<Route> routes = restSendToExtensionAction.routes();
        assertEquals(expected.size(), routes.size());
        List<String> expectedPaths = expected.stream().map(Route::getPath).collect(Collectors.toList());
        List<String> paths = routes.stream().map(Route::getPath).collect(Collectors.toList());
        List<Method> expectedMethods = expected.stream().map(Route::getMethod).collect(Collectors.toList());
        List<Method> methods = routes.stream().map(Route::getMethod).collect(Collectors.toList());
        assertTrue(paths.containsAll(expectedPaths));
        assertTrue(expectedPaths.containsAll(paths));
        assertTrue(methods.containsAll(expectedMethods));
        assertTrue(expectedMethods.containsAll(methods));
    }

    public void testRestSendToExtensionActionWithNamedRoute() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo foo", "PUT /bar bar", "POST /baz baz"),
            List.of("GET /deprecated/foo foo_deprecated", "It's deprecated!")
        );
        RestSendToExtensionAction restSendToExtensionAction = new RestSendToExtensionAction(
            registerRestActionRequest,
            discoveryExtensionNode,
            transportService,
            dynamicActionRegistry,
            identityService
        );

        assertEquals("uniqueid1:send_to_extension_action", restSendToExtensionAction.getName());
        List<NamedRoute> expected = new ArrayList<>();
        String uriPrefix = "/_extensions/_uniqueid1";
        NamedRoute nr1 = new NamedRoute.Builder().method(Method.GET).path(uriPrefix + "/foo").uniqueName("foo").build();

        NamedRoute nr2 = new NamedRoute.Builder().method(Method.PUT).path(uriPrefix + "/bar").uniqueName("bar").build();

        NamedRoute nr3 = new NamedRoute.Builder().method(Method.POST).path(uriPrefix + "/baz").uniqueName("baz").build();

        expected.add(nr1);
        expected.add(nr2);
        expected.add(nr3);

        List<Route> routes = restSendToExtensionAction.routes();
        assertEquals(expected.size(), routes.size());
        List<String> expectedPaths = expected.stream().map(Route::getPath).collect(Collectors.toList());
        List<String> paths = routes.stream().map(Route::getPath).collect(Collectors.toList());
        List<Method> expectedMethods = expected.stream().map(Route::getMethod).collect(Collectors.toList());
        List<Method> methods = routes.stream().map(Route::getMethod).collect(Collectors.toList());
        List<String> expectedNames = expected.stream().map(NamedRoute::name).collect(Collectors.toList());
        List<String> names = routes.stream().map(r -> ((NamedRoute) r).name()).collect(Collectors.toList());
        assertTrue(paths.containsAll(expectedPaths));
        assertTrue(expectedPaths.containsAll(paths));
        assertTrue(methods.containsAll(expectedMethods));
        assertTrue(expectedMethods.containsAll(methods));
        assertTrue(expectedNames.containsAll(names));
    }

    public void testRestSendToExtensionActionWithNamedRouteAndLegacyActionName() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of(
                "GET /foo foo cluster:admin/density/abc/foo",
                "PUT /bar bar cluster:admin/density/jkl/bar,cluster:admin/opendistro/mno/bar*",
                "POST /baz baz cluster:admin/density/xyz/baz"
            ),
            List.of("GET /deprecated/foo foo_deprecated cluster:admin/density/abc/foo_deprecated", "It's deprecated!")
        );
        RestSendToExtensionAction restSendToExtensionAction = new RestSendToExtensionAction(
            registerRestActionRequest,
            discoveryExtensionNode,
            transportService,
            dynamicActionRegistry,
            identityService
        );

        assertEquals("uniqueid1:send_to_extension_action", restSendToExtensionAction.getName());
        List<NamedRoute> expected = new ArrayList<>();
        String uriPrefix = "/_extensions/_uniqueid1";
        NamedRoute nr1 = new NamedRoute.Builder().method(Method.GET)
            .path(uriPrefix + "/foo")
            .uniqueName("foo")
            .legacyActionNames(Set.of("cluster:admin/density/abc/foo"))
            .build();
        NamedRoute nr2 = new NamedRoute.Builder().method(Method.PUT)
            .path(uriPrefix + "/bar")
            .uniqueName("bar")
            .legacyActionNames(Set.of("cluster:admin/density/jkl/bar", "cluster:admin/opendistro/mno/bar*"))
            .build();
        NamedRoute nr3 = new NamedRoute.Builder().method(Method.POST)
            .path(uriPrefix + "/baz")
            .uniqueName("baz")
            .legacyActionNames(Set.of("cluster:admin/density/xyz/baz"))
            .build();

        expected.add(nr1);
        expected.add(nr2);
        expected.add(nr3);

        List<Route> routes = restSendToExtensionAction.routes();
        assertEquals(expected.size(), routes.size());
        List<String> expectedPaths = expected.stream().map(Route::getPath).collect(Collectors.toList());
        List<String> paths = routes.stream().map(Route::getPath).collect(Collectors.toList());
        List<Method> expectedMethods = expected.stream().map(Route::getMethod).collect(Collectors.toList());
        List<Method> methods = routes.stream().map(Route::getMethod).collect(Collectors.toList());
        List<String> expectedNames = expected.stream().map(NamedRoute::name).collect(Collectors.toList());
        List<String> names = routes.stream().map(r -> ((NamedRoute) r).name()).collect(Collectors.toList());
        Set<String> expectedActionNames = expected.stream().flatMap(nr -> nr.actionNames().stream()).collect(Collectors.toSet());
        Set<String> actionNames = routes.stream().flatMap(nr -> ((NamedRoute) nr).actionNames().stream()).collect(Collectors.toSet());
        assertTrue(paths.containsAll(expectedPaths));
        assertTrue(expectedPaths.containsAll(paths));
        assertTrue(methods.containsAll(expectedMethods));
        assertTrue(expectedMethods.containsAll(methods));
        assertTrue(expectedNames.containsAll(names));
        assertTrue(expectedActionNames.containsAll(actionNames));
    }

    public void testRestSendToExtensionActionWithoutUniqueNameShouldFail() {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo", "PUT /bar"),
            List.of()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionMultipleNamedRoutesWithSameName() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo foo", "PUT /bar foo"),
            List.of()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionMultipleNamedRoutesWithSameLegacyActionName() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo foo cluster:admin/density/abc/foo", "PUT /bar bar cluster:admin/density/abc/foo"),
            List.of()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionMultipleRoutesWithSameMethodAndPath() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo", "GET /foo"),
            List.of()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionMultipleRoutesWithSameMethodAndPathWithDifferentPathParams() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo/{path_param1} fooWithParam", "GET /foo/{path_param2} listFooWithParam"),
            List.of()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionMultipleRoutesWithSameMethodAndPathWithPathParams() {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo/{path_param} fooWithParam", "GET /foo/{path_param}/list listFooWithParam"),
            List.of()
        );

        try {
            new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            );
        } catch (IllegalArgumentException e) {
            fail("IllegalArgumentException should not be thrown for different paths");
        }
    }

    public void testRestSendToExtensionWithNamedRouteCollidingWithDynamicTransportAction() throws Exception {
        DynamicActionRegistry dynamicActionRegistry = actionModule.getDynamicActionRegistry();
        ActionFilters emptyFilters = new ActionFilters(Collections.emptySet());
        ExtensionAction testExtensionAction = new ExtensionAction("extensionId", "test:action/name");
        ExtensionTransportAction testExtensionTransportAction = new ExtensionTransportAction("test:action/name", emptyFilters, null, null);
        assertNull(dynamicActionRegistry.get(testExtensionAction));
        dynamicActionRegistry.registerDynamicAction(testExtensionAction, testExtensionTransportAction);

        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo test:action/name"),
            List.of()
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionWithNamedRouteCollidingWithNativeTransportAction() throws Exception {
        actionModule.getDynamicActionRegistry()
            .registerUnmodifiableActionMap(Map.of(ClusterHealthAction.INSTANCE, mock(TransportClusterHealthAction.class)));
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo " + ClusterHealthAction.NAME),
            List.of()
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionActionFilterHeaders() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo foo", "PUT /bar bar", "POST /baz baz"),
            List.of("GET /deprecated/foo foo_deprecated", "It's deprecated!")
        );
        RestSendToExtensionAction restSendToExtensionAction = new RestSendToExtensionAction(
            registerRestActionRequest,
            discoveryExtensionNode,
            transportService,
            dynamicActionRegistry,
            identityService
        );

        Map<String, List<String>> headers = new HashMap<>();
        headers.put("Content-Type", Arrays.asList("application/json"));
        headers.put("Authorization", Arrays.asList("Bearer token"));
        headers.put("Proxy-Authorization", Arrays.asList("Basic credentials"));

        Set<String> allowList = Set.of("Content-Type"); // allowed headers
        Set<String> denyList = Set.of("Authorization", "Proxy-Authorization"); // denied headers

        Map<String, List<String>> filteredHeaders = restSendToExtensionAction.filterHeaders(headers, allowList, denyList);

        assertTrue(filteredHeaders.containsKey("Content-Type"));
        assertFalse(filteredHeaders.containsKey("Authorization"));
        assertFalse(filteredHeaders.containsKey("Proxy-Authorization"));
    }

    public void testRestSendToExtensionActionBadMethod() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("/foo", "PUT /bar", "POST /baz"),
            List.of("GET /deprecated/foo", "It's deprecated!")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionActionBadDeprecatedMethod() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo", "PUT /bar", "POST /baz"),
            List.of("/deprecated/foo", "It's deprecated!")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionActionMissingUri() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET", "PUT /bar", "POST /baz"),
            List.of("GET /deprecated/foo", "It's deprecated!")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }

    public void testRestSendToExtensionActionMissingDeprecatedUri() throws Exception {
        RegisterRestActionsRequest registerRestActionRequest = new RegisterRestActionsRequest(
            "uniqueid1",
            List.of("GET /foo", "PUT /bar", "POST /baz"),
            List.of("GET", "It's deprecated!")
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new RestSendToExtensionAction(
                registerRestActionRequest,
                discoveryExtensionNode,
                transportService,
                dynamicActionRegistry,
                identityService
            )
        );
    }
}
