/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.transport;

import org.density.Version;
import org.density.arrow.flight.api.flightinfo.FlightServerInfoAction;
import org.density.arrow.flight.api.flightinfo.NodesFlightInfoAction;
import org.density.arrow.flight.api.flightinfo.TransportNodesFlightInfoAction;
import org.density.arrow.flight.bootstrap.FlightService;
import org.density.arrow.flight.bootstrap.ServerComponents;
import org.density.arrow.flight.bootstrap.ServerConfig;
import org.density.arrow.flight.bootstrap.tls.DefaultSslContextProvider;
import org.density.arrow.flight.bootstrap.tls.SslContextProvider;
import org.density.arrow.flight.stats.FlightStatsAction;
import org.density.arrow.flight.stats.FlightStatsCollector;
import org.density.arrow.flight.stats.FlightStatsRestHandler;
import org.density.arrow.flight.stats.TransportFlightStatsAction;
import org.density.arrow.spi.StreamManager;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterService;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.common.util.FeatureFlags;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.env.NodeEnvironment;
import org.density.plugins.ActionPlugin;
import org.density.plugins.ClusterPlugin;
import org.density.plugins.ExtensiblePlugin;
import org.density.plugins.NetworkPlugin;
import org.density.plugins.Plugin;
import org.density.plugins.SecureTransportSettingsProvider;
import org.density.plugins.StreamManagerPlugin;
import org.density.repositories.RepositoriesService;
import org.density.rest.RestController;
import org.density.rest.RestHandler;
import org.density.script.ScriptService;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ExecutorBuilder;
import org.density.threadpool.ThreadPool;
import org.density.transport.AuxTransport;
import org.density.transport.Transport;
import org.density.transport.client.Client;
import org.density.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * FlightStreamPlugin class extends BaseFlightStreamPlugin and provides implementation for FlightStream plugin.
 */
public class FlightStreamPlugin extends Plugin
    implements
        StreamManagerPlugin,
        NetworkPlugin,
        ActionPlugin,
        ClusterPlugin,
        ExtensiblePlugin {

    private final FlightService flightService;
    private final boolean isArrowStreamsEnabled;
    private final boolean isStreamTransportEnabled;
    private FlightStatsCollector statsCollector;

    /**
     * Constructor for FlightStreamPluginImpl.
     * @param settings The settings for the FlightStreamPlugin.
     */
    public FlightStreamPlugin(Settings settings) {
        this.isArrowStreamsEnabled = FeatureFlags.isEnabled(FeatureFlags.ARROW_STREAMS);
        this.isStreamTransportEnabled = FeatureFlags.isEnabled(FeatureFlags.STREAM_TRANSPORT);
        if (isStreamTransportEnabled || isArrowStreamsEnabled) {
            try {
                ServerConfig.init(settings);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize Arrow Flight server", e);
            }
        }
        this.flightService = isArrowStreamsEnabled ? new FlightService(settings) : null;
    }

    /**
     * Creates components for the FlightStream plugin.
     * @param client The client instance.
     * @param clusterService The cluster service instance.
     * @param threadPool The thread pool instance.
     * @param resourceWatcherService The resource watcher service instance.
     * @param scriptService The script service instance.
     * @param xContentRegistry The named XContent registry.
     * @param environment The environment instance.
     * @param nodeEnvironment The node environment instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param repositoriesServiceSupplier The supplier for the repositories service.
     * @return FlightService
     */
    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        if (!isArrowStreamsEnabled && !isStreamTransportEnabled) {
            return Collections.emptyList();
        }

        List<Object> components = new ArrayList<>();

        if (isArrowStreamsEnabled) {
            flightService.setClusterService(clusterService);
            flightService.setThreadPool(threadPool);
            flightService.setClient(client);
        }
        if (isStreamTransportEnabled) {
            statsCollector = new FlightStatsCollector();
            components.add(statsCollector);
        }
        return components;
    }

    /**
     * Gets the secure transports for the FlightStream plugin.
     * @param settings The settings for the plugin.
     * @param threadPool The thread pool instance.
     * @param pageCacheRecycler The page cache recycler instance.
     * @param circuitBreakerService The circuit breaker service instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param networkService The network service instance.
     * @param secureTransportSettingsProvider The secure transport settings provider.
     * @param tracer The tracer instance.
     * @return A map of secure transports.
     */
    @Override
    public Map<String, Supplier<Transport>> getSecureTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        SecureTransportSettingsProvider secureTransportSettingsProvider,
        Tracer tracer
    ) {
        if (isArrowStreamsEnabled && ServerConfig.isSslEnabled()) {
            flightService.setSecureTransportSettingsProvider(secureTransportSettingsProvider);
        }
        if (isStreamTransportEnabled && ServerConfig.isSslEnabled()) {
            SslContextProvider sslContextProvider = new DefaultSslContextProvider(secureTransportSettingsProvider);
            return Collections.singletonMap(
                "FLIGHT-SECURE",
                () -> new FlightTransport(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    pageCacheRecycler,
                    circuitBreakerService,
                    namedWriteableRegistry,
                    networkService,
                    tracer,
                    sslContextProvider,
                    statsCollector
                )
            );
        }
        return Collections.emptyMap();
    }

    /**
     * Gets the secure transports for the FlightStream plugin.
     * @param settings The settings for the plugin.
     * @param threadPool The thread pool instance.
     * @param pageCacheRecycler The page cache recycler instance.
     * @param circuitBreakerService The circuit breaker service instance.
     * @param namedWriteableRegistry The named writeable registry.
     * @param networkService The network service instance.
     * @param tracer The tracer instance.
     * @return A map of secure transports.
     */
    @Override
    public Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        Tracer tracer
    ) {
        if (isStreamTransportEnabled && !ServerConfig.isSslEnabled()) {
            return Collections.singletonMap(
                "FLIGHT",
                () -> new FlightTransport(
                    settings,
                    Version.CURRENT,
                    threadPool,
                    pageCacheRecycler,
                    circuitBreakerService,
                    namedWriteableRegistry,
                    networkService,
                    tracer,
                    null,
                    statsCollector
                )
            );
        }
        return Collections.emptyMap();
    }

    /**
     * Gets the auxiliary transports for the FlightStream plugin.
     * @param settings The settings for the plugin.
     * @param threadPool The thread pool instance.
     * @param circuitBreakerService The circuit breaker service instance.
     * @param networkService The network service instance.
     * @param clusterSettings The cluster settings instance.
     * @param tracer The tracer instance.
     * @return A map of auxiliary transports.
     */
    @Override
    public Map<String, Supplier<AuxTransport>> getAuxTransports(
        Settings settings,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        if (isArrowStreamsEnabled) {
            flightService.setNetworkService(networkService);
            return Collections.singletonMap(flightService.settingKey(), () -> flightService);
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Gets the REST handlers for the FlightStream plugin.
     * @param settings The settings for the plugin.
     * @param restController The REST controller instance.
     * @param clusterSettings The cluster settings instance.
     * @param indexScopedSettings The index scoped settings instance.
     * @param settingsFilter The settings filter instance.
     * @param indexNameExpressionResolver The index name expression resolver instance.
     * @param nodesInCluster The supplier for the discovery nodes.
     * @return A list of REST handlers.
     */
    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        List<RestHandler> handlers = new ArrayList<>();

        if (isArrowStreamsEnabled) {
            handlers.add(new FlightServerInfoAction());
        }

        if (isStreamTransportEnabled) {
            handlers.add(new FlightStatsRestHandler());
        }

        return handlers;
    }

    /**
     * Gets the list of action handlers for the FlightStream plugin.
     * @return A list of action handlers.
     */
    @Override
    public List<ActionHandler<?, ?>> getActions() {
        List<ActionHandler<?, ?>> actions = new ArrayList<>();

        if (isArrowStreamsEnabled) {
            actions.add(new ActionHandler<>(NodesFlightInfoAction.INSTANCE, TransportNodesFlightInfoAction.class));
        }

        if (isStreamTransportEnabled) {
            actions.add(new ActionHandler<>(FlightStatsAction.INSTANCE, TransportFlightStatsAction.class));
        }

        return actions;
    }

    /**
     * Called when node is started. DiscoveryNode argument is passed to allow referring localNode value inside plugin
     *
     * @param localNode local Node info
     */
    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        if (isArrowStreamsEnabled) {
            flightService.getFlightClientManager().buildClientAsync(localNode.getId());
        }
    }

    /**
     * Gets the StreamManager instance for managing flight streams.
     */
    @Override
    public Optional<StreamManager> getStreamManager() {
        return isArrowStreamsEnabled ? Optional.ofNullable(flightService.getStreamManager()) : Optional.empty();
    }

    /**
     * Gets the list of ExecutorBuilder instances for building thread pools used for FlightServer.
     * @param settings The settings for the plugin
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        if (!isArrowStreamsEnabled && !isStreamTransportEnabled) {
            return Collections.emptyList();
        }
        return List.of(
            ServerConfig.getServerExecutorBuilder(),
            ServerConfig.getGrpcExecutorBuilder(),
            ServerConfig.getClientExecutorBuilder()
        );
    }

    /**
     * Gets the list of settings for the Flight plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        if (!isArrowStreamsEnabled && !isStreamTransportEnabled) {
            return Collections.emptyList();
        }
        return new ArrayList<>(
            Arrays.asList(
                ServerComponents.SETTING_FLIGHT_PORTS,
                ServerComponents.SETTING_FLIGHT_HOST,
                ServerComponents.SETTING_FLIGHT_BIND_HOST,
                ServerComponents.SETTING_FLIGHT_PUBLISH_HOST
            )
        ) {
            {
                addAll(ServerConfig.getSettings());
            }
        };
    }
}
