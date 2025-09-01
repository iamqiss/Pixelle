/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.action;

import com.google.protobuf.ByteString;
import org.density.Version;
import org.density.action.ActionModule;
import org.density.action.ActionModule.DynamicActionRegistry;
import org.density.action.support.ActionFilters;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.network.NetworkService;
import org.density.common.settings.Settings;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.transport.TransportAddress;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.extensions.AcknowledgedResponse;
import org.density.extensions.DiscoveryExtensionNode;
import org.density.extensions.rest.RestSendToExtensionActionTests;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.client.NoOpNodeClient;
import org.density.test.transport.MockTransportService;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.ActionNotFoundTransportException;
import org.density.transport.NodeNotConnectedException;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;
import org.density.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExtensionTransportActionsHandlerTests extends DensityTestCase {
    private static final ActionFilters EMPTY_FILTERS = new ActionFilters(Collections.emptySet());
    private TransportService transportService;
    private MockNioTransport transport;
    private DiscoveryExtensionNode discoveryExtensionNode;
    private ExtensionTransportActionsHandler extensionTransportActionsHandler;
    private NodeClient client;
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
        client = new NoOpNodeClient(this.getTestName());
        ActionModule mockActionModule = mock(ActionModule.class);
        DynamicActionRegistry dynamicActionRegistry = new DynamicActionRegistry();
        dynamicActionRegistry.registerUnmodifiableActionMap(Collections.emptyMap());
        when(mockActionModule.getDynamicActionRegistry()).thenReturn(dynamicActionRegistry);
        when(mockActionModule.getActionFilters()).thenReturn(EMPTY_FILTERS);
        extensionTransportActionsHandler = new ExtensionTransportActionsHandler(
            Map.of("uniqueid1", discoveryExtensionNode),
            transportService,
            client,
            mockActionModule,
            null
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        client.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRegisterAction() {
        String action = "test-action";
        extensionTransportActionsHandler.registerAction(action, discoveryExtensionNode.getId());
        assertEquals(discoveryExtensionNode, extensionTransportActionsHandler.getExtension(action));

        // Test duplicate action registration
        expectThrows(
            IllegalArgumentException.class,
            () -> extensionTransportActionsHandler.registerAction(action, discoveryExtensionNode.getId())
        );
        assertEquals(discoveryExtensionNode, extensionTransportActionsHandler.getExtension(action));
    }

    public void testRegisterTransportActionsRequest() {
        String action = "test-action";
        RegisterTransportActionsRequest request = new RegisterTransportActionsRequest("uniqueid1", Set.of(action));
        AcknowledgedResponse response = (AcknowledgedResponse) extensionTransportActionsHandler.handleRegisterTransportActionsRequest(
            request
        );
        assertTrue(response.getStatus());
        assertEquals(discoveryExtensionNode, extensionTransportActionsHandler.getExtension(action));

        // Test duplicate action registration
        response = (AcknowledgedResponse) extensionTransportActionsHandler.handleRegisterTransportActionsRequest(request);
        assertFalse(response.getStatus());
    }

    public void testTransportActionRequestFromExtension() throws Exception {
        String action = "test-action";
        ByteString requestBytes = ByteString.copyFromUtf8("requestBytes");
        TransportActionRequestFromExtension request = new TransportActionRequestFromExtension(action, requestBytes, "uniqueid1");
        RemoteExtensionActionResponse response = extensionTransportActionsHandler.handleTransportActionRequestFromExtension(request);
        assertFalse(response.isSuccess());
        String responseString = response.getResponseBytesAsString();
        assertEquals("Request failed: action [test-action] is not registered for any extension.", responseString);
    }

    public void testSendTransportRequestToExtension() throws InterruptedException {
        String action = "test-action";
        ByteString requestBytes = ByteString.copyFromUtf8("requestBytes");
        ExtensionActionRequest request = new ExtensionActionRequest(action, requestBytes);

        // Action not registered, expect exception
        expectThrows(
            ActionNotFoundTransportException.class,
            () -> extensionTransportActionsHandler.sendTransportRequestToExtension(request)
        );

        // Register Action
        RegisterTransportActionsRequest registerRequest = new RegisterTransportActionsRequest("uniqueid1", Set.of(action));
        AcknowledgedResponse response = (AcknowledgedResponse) extensionTransportActionsHandler.handleRegisterTransportActionsRequest(
            registerRequest
        );
        assertTrue(response.getStatus());

        expectThrows(NodeNotConnectedException.class, () -> extensionTransportActionsHandler.sendTransportRequestToExtension(request));
    }
}
