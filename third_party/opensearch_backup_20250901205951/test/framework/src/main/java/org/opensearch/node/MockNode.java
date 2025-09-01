/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.node;

import org.density.Version;
import org.density.cluster.ClusterInfoService;
import org.density.cluster.MockInternalClusterInfoService;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.service.ClusterService;
import org.density.common.Nullable;
import org.density.common.network.NetworkModule;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.BigArrays;
import org.density.common.util.MockBigArrays;
import org.density.common.util.MockPageCacheRecycler;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.transport.BoundTransportAddress;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.env.Environment;
import org.density.http.HttpServerTransport;
import org.density.indices.IndicesService;
import org.density.plugins.Plugin;
import org.density.plugins.PluginInfo;
import org.density.plugins.SearchPlugin;
import org.density.script.MockScriptService;
import org.density.script.ScriptContext;
import org.density.script.ScriptEngine;
import org.density.script.ScriptService;
import org.density.search.MockSearchService;
import org.density.search.SearchService;
import org.density.search.deciders.ConcurrentSearchRequestDecider;
import org.density.search.fetch.FetchPhase;
import org.density.search.query.QueryPhase;
import org.density.tasks.TaskResourceTrackingService;
import org.density.telemetry.tracing.Tracer;
import org.density.test.MockHttpTransport;
import org.density.test.transport.MockTransportService;
import org.density.threadpool.ThreadPool;
import org.density.transport.Transport;
import org.density.transport.TransportInterceptor;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A node for testing which allows:
 * <ul>
 *   <li>Overriding Version.CURRENT</li>
 *   <li>Adding test plugins that exist on the classpath</li>
 * </ul>
 */
public class MockNode extends Node {

    private final Collection<PluginInfo> classpathPlugins;

    private MockNode(
        final Environment environment,
        final Collection<PluginInfo> classpathPlugins,
        final boolean forbidPrivateIndexSettings
    ) {
        super(environment, classpathPlugins, forbidPrivateIndexSettings);
        this.classpathPlugins = classpathPlugins;
    }

    public MockNode(
        final Settings settings,
        final Collection<PluginInfo> classpathPlugins,
        final Path configPath,
        final boolean forbidPrivateIndexSettings
    ) {
        this(
            InternalSettingsPreparer.prepareEnvironment(settings, Collections.emptyMap(), configPath, () -> "mock_ node"),
            classpathPlugins,
            forbidPrivateIndexSettings
        );
    }

    public MockNode(final Settings settings, final Collection<Class<? extends Plugin>> classpathPlugins) {
        this(
            InternalSettingsPreparer.prepareEnvironment(settings, Collections.emptyMap(), null, () -> "mock_ node"),
            classpathPlugins.stream()
                .map(
                    p -> new PluginInfo(
                        p.getName(),
                        "classpath plugin",
                        "NA",
                        Version.CURRENT,
                        "1.8",
                        p.getName(),
                        null,
                        Collections.emptyList(),
                        false
                    )
                )
                .collect(Collectors.toList()),
            true
        );
    }

    /**
     * The classpath plugins this node was constructed with.
     */
    public Collection<PluginInfo> getClasspathPlugins() {
        return classpathPlugins;
    }

    @Override
    protected BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        if (getPluginsService().filterPlugins(NodeMocksPlugin.class).isEmpty()) {
            return super.createBigArrays(pageCacheRecycler, circuitBreakerService);
        }
        return new MockBigArrays(pageCacheRecycler, circuitBreakerService);
    }

    @Override
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        if (getPluginsService().filterPlugins(NodeMocksPlugin.class).isEmpty()) {
            return super.createPageCacheRecycler(settings);
        }
        return new MockPageCacheRecycler(settings);
    }

    @Override
    protected SearchService newSearchService(
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ScriptService scriptService,
        BigArrays bigArrays,
        QueryPhase queryPhase,
        FetchPhase fetchPhase,
        ResponseCollectorService responseCollectorService,
        CircuitBreakerService circuitBreakerService,
        Executor indexSearcherExecutor,
        TaskResourceTrackingService taskResourceTrackingService,
        Collection<ConcurrentSearchRequestDecider.Factory> concurrentSearchDeciderFactories,
        List<SearchPlugin.ProfileMetricsProvider> pluginProfilers
    ) {
        if (getPluginsService().filterPlugins(MockSearchService.TestPlugin.class).isEmpty()) {
            return super.newSearchService(
                clusterService,
                indicesService,
                threadPool,
                scriptService,
                bigArrays,
                queryPhase,
                fetchPhase,
                responseCollectorService,
                circuitBreakerService,
                indexSearcherExecutor,
                taskResourceTrackingService,
                concurrentSearchDeciderFactories,
                pluginProfilers
            );
        }
        return new MockSearchService(
            clusterService,
            indicesService,
            threadPool,
            scriptService,
            bigArrays,
            queryPhase,
            fetchPhase,
            circuitBreakerService,
            indexSearcherExecutor,
            taskResourceTrackingService
        );
    }

    @Override
    protected ScriptService newScriptService(Settings settings, Map<String, ScriptEngine> engines, Map<String, ScriptContext<?>> contexts) {
        if (getPluginsService().filterPlugins(MockScriptService.TestPlugin.class).isEmpty()) {
            return super.newScriptService(settings, engines, contexts);
        }
        return new MockScriptService(settings, engines, contexts);
    }

    @Override
    protected TransportService newTransportService(
        Settings settings,
        Transport transport,
        @Nullable Transport streamTransport,
        ThreadPool threadPool,
        TransportInterceptor interceptor,
        Function<BoundTransportAddress, DiscoveryNode> localNodeFactory,
        ClusterSettings clusterSettings,
        Set<String> taskHeaders,
        Tracer tracer
    ) {
        // we use the MockTransportService.TestPlugin class as a marker to create a network
        // module with this MockNetworkService. NetworkService is such an integral part of the systme
        // we don't allow to plug it in from plugins or anything. this is a test-only override and
        // can't be done in a production env.
        if (getPluginsService().filterPlugins(MockTransportService.TestPlugin.class).isEmpty()) {
            return super.newTransportService(
                settings,
                transport,
                streamTransport,
                threadPool,
                interceptor,
                localNodeFactory,
                clusterSettings,
                taskHeaders,
                tracer
            );
        } else {
            return new MockTransportService(
                settings,
                transport,
                streamTransport,
                threadPool,
                interceptor,
                localNodeFactory,
                clusterSettings,
                taskHeaders,
                tracer
            );
        }
    }

    @Override
    protected ClusterInfoService newClusterInfoService(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        NodeClient client
    ) {
        if (getPluginsService().filterPlugins(MockInternalClusterInfoService.TestPlugin.class).isEmpty()) {
            return super.newClusterInfoService(settings, clusterService, threadPool, client);
        } else {
            final MockInternalClusterInfoService service = new MockInternalClusterInfoService(settings, clusterService, threadPool, client);
            clusterService.addListener(service);
            return service;
        }
    }

    @Override
    protected HttpServerTransport newHttpTransport(NetworkModule networkModule) {
        if (getPluginsService().filterPlugins(MockHttpTransport.TestPlugin.class).isEmpty()) {
            return super.newHttpTransport(networkModule);
        } else {
            return new MockHttpTransport();
        }
    }

    @Override
    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        // do not configure this in tests as this is causing SetOnce to throw exceptions when jvm is used for multiple tests
    }

    public NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
