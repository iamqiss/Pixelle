/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.reactor;

import org.density.common.SetOnce;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.util.BigArrays;
import org.density.common.util.PageCacheRecycler;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.http.HttpServerTransport;
import org.density.http.HttpServerTransport.Dispatcher;
import org.density.http.reactor.netty4.ReactorNetty4HttpServerTransport;
import org.density.plugins.NetworkPlugin;
import org.density.plugins.Plugin;
import org.density.plugins.SecureHttpTransportSettingsProvider;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The experimental network plugin that introduces new transport implementations based on Reactor Netty.
 */
public class ReactorNetty4Plugin extends Plugin implements NetworkPlugin {
    /**
     * The name of new experimental HTTP transport implementations based on Reactor Netty.
     */
    public static final String REACTOR_NETTY_HTTP_TRANSPORT_NAME = "reactor-netty4";

    /**
     * The name of new experimental secure HTTP transport implementations based on Reactor Netty.
     */
    public static final String REACTOR_NETTY_SECURE_HTTP_TRANSPORT_NAME = "reactor-netty4-secure";

    private final SetOnce<SharedGroupFactory> groupFactory = new SetOnce<>();

    /**
     * Default constructor
     */
    public ReactorNetty4Plugin() {}

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(ReactorNetty4HttpServerTransport.SETTING_H2C_MAX_CONTENT_LENGTH);
    }

    /**
     * Returns a map of {@link HttpServerTransport} suppliers.
     * See {@link org.density.common.network.NetworkModule#HTTP_TYPE_SETTING} to configure a specific implementation.
     * @param settings settings
     * @param networkService network service
     * @param bigArrays big array allocator
     * @param pageCacheRecycler page cache recycler instance
     * @param circuitBreakerService circuit breaker service instance
     * @param threadPool thread pool instance
     * @param xContentRegistry XContent registry instance
     * @param dispatcher dispatcher instance
     * @param clusterSettings cluster settings
     * @param tracer tracer instance
     */
    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        return Collections.singletonMap(
            REACTOR_NETTY_HTTP_TRANSPORT_NAME,
            () -> new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                getSharedGroupFactory(settings),
                null, /* no security settings provider */
                tracer
            )
        );
    }

    /**
     * Returns a map of {@link HttpServerTransport} suppliers.
     * See {@link org.density.common.network.NetworkModule#HTTP_TYPE_SETTING} to configure a specific implementation.
     * @param settings settings
     * @param networkService network service
     * @param bigArrays big array allocator
     * @param pageCacheRecycler page cache recycler instance
     * @param circuitBreakerService circuit breaker service instance
     * @param threadPool thread pool instance
     * @param xContentRegistry XContent registry instance
     * @param dispatcher dispatcher instance
     * @param clusterSettings cluster settings
     * @param secureHttpTransportSettingsProvider secure HTTP transport settings provider
     * @param tracer tracer instance
     */
    @Override
    public Map<String, Supplier<HttpServerTransport>> getSecureHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        SecureHttpTransportSettingsProvider secureHttpTransportSettingsProvider,
        Tracer tracer
    ) {
        return Collections.singletonMap(
            REACTOR_NETTY_SECURE_HTTP_TRANSPORT_NAME,
            () -> new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                getSharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                tracer
            )
        );
    }

    private SharedGroupFactory getSharedGroupFactory(Settings settings) {
        final SharedGroupFactory groupFactory = this.groupFactory.get();
        if (groupFactory != null) {
            assert groupFactory.getSettings().equals(settings) : "Different settings than originally provided";
            return groupFactory;
        } else {
            this.groupFactory.set(new SharedGroupFactory(settings));
            return this.groupFactory.get();
        }
    }
}
