/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.rest;

import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.network.NetworkService;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.bytes.BytesArray;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.extensions.DiscoveryExtensionNode;
import org.density.extensions.ExtensionsManager;
import org.density.extensions.ExtensionsSettings.Extension;
import org.density.identity.IdentityService;
import org.density.rest.RestRequest;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.rest.FakeRestChannel;
import org.density.test.rest.FakeRestRequest;
import org.density.test.transport.MockTransportService;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.nio.MockNioTransport;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.mockito.Mockito;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class RestInitializeExtensionActionTests extends DensityTestCase {

    private TransportService transportService;
    private MockNioTransport transport;
    private final ThreadPool threadPool = new TestThreadPool(RestInitializeExtensionActionTests.class.getSimpleName());

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

    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testRestInitializeExtensionActionResponse() throws Exception {
        ExtensionsManager extensionsManager = mock(ExtensionsManager.class);
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(extensionsManager);
        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"ad-extension\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"densityVersion\":\""
            + Version.CURRENT.toString()
            + "\","
            + "\"minimumCompatibleVersion\":\""
            + Version.CURRENT.minimumCompatibilityVersion().toString()
            + "\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), MediaTypeRegistry.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(channel.capturedResponse().status(), RestStatus.ACCEPTED);
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("A request to initialize an extension has been sent."));
    }

    public void testRestInitializeExtensionActionFailure() throws Exception {
        ExtensionsManager extensionsManager = new ExtensionsManager(Set.of(), new IdentityService(Settings.EMPTY, threadPool, List.of()));
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(extensionsManager);

        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"densityVersion\":\""
            + Version.CURRENT.toString()
            + "\","
            + "\"minimumCompatibleVersion\":\""
            + Version.CURRENT.minimumCompatibilityVersion().toString()
            + "\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), MediaTypeRegistry.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(1, channel.errors().get());
        assertTrue(
            channel.capturedResponse().content().utf8ToString().contains("Required field [extension uniqueId] is missing in the request")
        );
    }

    public void testRestInitializeExtensionActionResponseWithAdditionalSettings() throws Exception {
        Setting boolSetting = Setting.boolSetting("boolSetting", false, Setting.Property.ExtensionScope);
        Setting stringSetting = Setting.simpleString("stringSetting", "default", Setting.Property.ExtensionScope);
        Setting intSetting = Setting.intSetting("intSetting", 0, Setting.Property.ExtensionScope);
        Setting listSetting = Setting.listSetting(
            "listSetting",
            List.of("first", "second", "third"),
            Function.identity(),
            Setting.Property.ExtensionScope
        );
        ExtensionsManager extensionsManager = new ExtensionsManager(
            Set.of(boolSetting, stringSetting, intSetting, listSetting),
            new IdentityService(Settings.EMPTY, threadPool, List.of())
        );
        ExtensionsManager spy = spy(extensionsManager);

        // optionally, you can stub out some methods:
        when(spy.getAdditionalSettings()).thenCallRealMethod();
        Mockito.doCallRealMethod().when(spy).loadExtension(any(Extension.class));
        Mockito.doNothing().when(spy).initializeExtensionNode(any(DiscoveryExtensionNode.class));
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(spy);
        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"ad-extension\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"densityVersion\":\""
            + Version.CURRENT.toString()
            + "\","
            + "\"minimumCompatibleVersion\":\""
            + Version.CURRENT.minimumCompatibilityVersion().toString()
            + "\",\"boolSetting\":true,\"stringSetting\":\"customSetting\",\"intSetting\":5,\"listSetting\":[\"one\",\"two\",\"three\"]}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), MediaTypeRegistry.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(RestStatus.ACCEPTED, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("A request to initialize an extension has been sent."));

        Optional<Extension> extension = spy.lookupExtensionSettingsById("ad-extension");
        assertTrue(extension.isPresent());
        assertEquals(true, extension.get().getAdditionalSettings().get(boolSetting));
        assertEquals("customSetting", extension.get().getAdditionalSettings().get(stringSetting));
        assertEquals(5, extension.get().getAdditionalSettings().get(intSetting));

        List<String> listSettingValue = (List<String>) extension.get().getAdditionalSettings().get(listSetting);
        assertTrue(listSettingValue.contains("one"));
        assertTrue(listSettingValue.contains("two"));
        assertTrue(listSettingValue.contains("three"));
    }

    public void testRestInitializeExtensionActionResponseWithAdditionalSettingsUsingDefault() throws Exception {
        Setting boolSetting = Setting.boolSetting("boolSetting", false, Setting.Property.ExtensionScope);
        Setting stringSetting = Setting.simpleString("stringSetting", "default", Setting.Property.ExtensionScope);
        Setting intSetting = Setting.intSetting("intSetting", 0, Setting.Property.ExtensionScope);
        Setting listSetting = Setting.listSetting(
            "listSetting",
            List.of("first", "second", "third"),
            Function.identity(),
            Setting.Property.ExtensionScope
        );
        ExtensionsManager extensionsManager = new ExtensionsManager(
            Set.of(boolSetting, stringSetting, intSetting, listSetting),
            new IdentityService(Settings.EMPTY, threadPool, List.of())
        );
        ExtensionsManager spy = spy(extensionsManager);

        // optionally, you can stub out some methods:
        when(spy.getAdditionalSettings()).thenCallRealMethod();
        Mockito.doCallRealMethod().when(spy).loadExtension(any(Extension.class));
        Mockito.doNothing().when(spy).initializeExtensionNode(any(DiscoveryExtensionNode.class));
        RestInitializeExtensionAction restInitializeExtensionAction = new RestInitializeExtensionAction(spy);
        final String content = "{\"name\":\"ad-extension\",\"uniqueId\":\"ad-extension\",\"hostAddress\":\"127.0.0.1\","
            + "\"port\":\"4532\",\"version\":\"1.0\",\"densityVersion\":\""
            + Version.CURRENT.toString()
            + "\","
            + "\"minimumCompatibleVersion\":\""
            + Version.CURRENT.minimumCompatibilityVersion().toString()
            + "\"}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), MediaTypeRegistry.JSON)
            .withMethod(RestRequest.Method.POST)
            .build();

        FakeRestChannel channel = new FakeRestChannel(request, false, 0);
        restInitializeExtensionAction.handleRequest(request, channel, null);

        assertEquals(RestStatus.ACCEPTED, channel.capturedResponse().status());
        assertTrue(channel.capturedResponse().content().utf8ToString().contains("A request to initialize an extension has been sent."));

        Optional<Extension> extension = spy.lookupExtensionSettingsById("ad-extension");
        assertTrue(extension.isPresent());
        assertEquals(false, extension.get().getAdditionalSettings().get(boolSetting));
        assertEquals("default", extension.get().getAdditionalSettings().get(stringSetting));
        assertEquals(0, extension.get().getAdditionalSettings().get(intSetting));

        List<String> listSettingValue = (List<String>) extension.get().getAdditionalSettings().get(listSetting);
        assertTrue(listSettingValue.contains("first"));
        assertTrue(listSettingValue.contains("second"));
        assertTrue(listSettingValue.contains("third"));
    }

}
