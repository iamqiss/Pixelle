/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.http.netty4.ssl;

import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.MockBigArrays;
import org.density.common.util.MockPageCacheRecycler;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.http.HttpServerTransport;
import org.density.http.NullDispatcher;
import org.density.plugins.SecureHttpTransportSettingsProvider;
import org.density.plugins.TransportExceptionHandler;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.SharedGroupFactory;
import org.density.transport.TransportAdapterProvider;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.netty.channel.ChannelInboundHandlerAdapter;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link SecureNetty4HttpServerTransport} class.
 */
public class SecureNetty4HttpServerTransportConfigurationTests extends DensityTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private ClusterSettings clusterSettings;

    private static class ConfigurableSecureHttpTransportSettingsProvider implements SecureHttpTransportSettingsProvider {
        private final List<TransportAdapterProvider<HttpServerTransport>> transportAdapterProviders;

        public ConfigurableSecureHttpTransportSettingsProvider(
            List<TransportAdapterProvider<HttpServerTransport>> transportAdapterProviders
        ) {
            this.transportAdapterProviders = transportAdapterProviders;
        }

        @Override
        public Collection<TransportAdapterProvider<HttpServerTransport>> getHttpTransportAdapterProviders(Settings settings) {
            return transportAdapterProviders;
        }

        @Override
        public Optional<TransportExceptionHandler> buildHttpServerExceptionHandler(Settings settings, HttpServerTransport transport) {
            return Optional.empty();
        }

        @Override
        public Optional<SSLEngine> buildSecureHttpServerEngine(Settings settings, HttpServerTransport transport) throws SSLException {
            return Optional.empty();
        }
    }

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        bigArrays = null;
        clusterSettings = null;
    }

    public void testRequestHeaderVerifier() throws InterruptedException {
        final TransportAdapterProvider<HttpServerTransport> transportAdapterProvider = new TransportAdapterProvider<HttpServerTransport>() {
            @Override
            public String name() {
                return SecureNetty4HttpServerTransport.REQUEST_HEADER_VERIFIER;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <C> Optional<C> create(Settings settings, HttpServerTransport transport, Class<C> adapterClass) {
                return Optional.of((C) new ChannelInboundHandlerAdapter());
            }

        };

        try (
            final SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                new ConfigurableSecureHttpTransportSettingsProvider(List.of(transportAdapterProvider)),
                NoopTracer.INSTANCE
            )
        ) {

        }
    }

    public void testMultipleRequestHeaderVerifiers() throws InterruptedException {
        final TransportAdapterProvider<HttpServerTransport> transportAdapterProvider = new TransportAdapterProvider<HttpServerTransport>() {
            @Override
            public String name() {
                return SecureNetty4HttpServerTransport.REQUEST_HEADER_VERIFIER;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <C> Optional<C> create(Settings settings, HttpServerTransport transport, Class<C> adapterClass) {
                return Optional.of((C) new ChannelInboundHandlerAdapter());
            }

        };

        final IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new SecureNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                new ConfigurableSecureHttpTransportSettingsProvider(List.of(transportAdapterProvider, transportAdapterProvider)),
                NoopTracer.INSTANCE
            )
        );

        assertThat(ex.getMessage(), equalTo("Cannot have more than one header verifier configured, supplied 2"));
    }

    public void testRequestDecompressor() throws InterruptedException {
        final TransportAdapterProvider<HttpServerTransport> transportAdapterProvider = new TransportAdapterProvider<HttpServerTransport>() {
            @Override
            public String name() {
                return SecureNetty4HttpServerTransport.REQUEST_DECOMPRESSOR;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <C> Optional<C> create(Settings settings, HttpServerTransport transport, Class<C> adapterClass) {
                return Optional.of((C) new ChannelInboundHandlerAdapter());
            }

        };

        try (
            final SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                new ConfigurableSecureHttpTransportSettingsProvider(List.of(transportAdapterProvider)),
                NoopTracer.INSTANCE
            )
        ) {

        }
    }

    public void testRequestDecompressorAndRequestHeaderVerifier() throws InterruptedException {
        final TransportAdapterProvider<HttpServerTransport> requestDecompressor = new TransportAdapterProvider<HttpServerTransport>() {
            @Override
            public String name() {
                return SecureNetty4HttpServerTransport.REQUEST_DECOMPRESSOR;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <C> Optional<C> create(Settings settings, HttpServerTransport transport, Class<C> adapterClass) {
                return Optional.of((C) new ChannelInboundHandlerAdapter());
            }

        };

        final TransportAdapterProvider<HttpServerTransport> requestHeaderVerifier = new TransportAdapterProvider<HttpServerTransport>() {
            @Override
            public String name() {
                return SecureNetty4HttpServerTransport.REQUEST_HEADER_VERIFIER;
            }

            @SuppressWarnings("unchecked")
            @Override
            public <C> Optional<C> create(Settings settings, HttpServerTransport transport, Class<C> adapterClass) {
                return Optional.of((C) new ChannelInboundHandlerAdapter());
            }

        };

        try (
            final SecureNetty4HttpServerTransport transport = new SecureNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                new ConfigurableSecureHttpTransportSettingsProvider(List.of(requestDecompressor, requestHeaderVerifier)),
                NoopTracer.INSTANCE
            )
        ) {

        }
    }
}
