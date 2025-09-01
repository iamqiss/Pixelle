/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc;

import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.env.NodeEnvironment;
import org.density.plugins.ExtensiblePlugin;
import org.density.plugins.NetworkPlugin;
import org.density.plugins.Plugin;
import org.density.plugins.SecureAuxTransportSettingsProvider;
import org.density.repositories.RepositoriesService;
import org.density.script.ScriptService;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;
import org.density.transport.AuxTransport;
import org.density.transport.client.Client;
import org.density.transport.grpc.proto.request.search.query.AbstractQueryBuilderProtoUtils;
import org.density.transport.grpc.proto.request.search.query.QueryBuilderProtoConverter;
import org.density.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistry;
import org.density.transport.grpc.services.DocumentServiceImpl;
import org.density.transport.grpc.services.SearchServiceImpl;
import org.density.transport.grpc.ssl.SecureNetty4GrpcServerTransport;
import org.density.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import io.grpc.BindableService;

import static org.density.transport.grpc.Netty4GrpcServerTransport.GRPC_TRANSPORT_SETTING_KEY;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_BIND_HOST;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_HOST;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_KEEPALIVE_TIMEOUT;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONNECTION_AGE;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_CONNECTION_IDLE;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_MAX_MSG_SIZE;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PORT;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_HOST;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_PUBLISH_PORT;
import static org.density.transport.grpc.Netty4GrpcServerTransport.SETTING_GRPC_WORKER_COUNT;
import static org.density.transport.grpc.ssl.SecureNetty4GrpcServerTransport.GRPC_SECURE_TRANSPORT_SETTING_KEY;
import static org.density.transport.grpc.ssl.SecureNetty4GrpcServerTransport.SETTING_GRPC_SECURE_PORT;

/**
 * Main class for the gRPC plugin.
 */
public final class GrpcPlugin extends Plugin implements NetworkPlugin, ExtensiblePlugin {

    private Client client;
    private final List<QueryBuilderProtoConverter> queryConverters = new ArrayList<>();
    private QueryBuilderProtoConverterRegistry queryRegistry;
    private AbstractQueryBuilderProtoUtils queryUtils;

    /**
     * Creates a new GrpcPlugin instance.
     */
    public GrpcPlugin() {}

    /**
     * Loads extensions from other plugins.
     * This method is called by the Density plugin system to load extensions from other plugins.
     *
     * @param loader The extension loader to use for loading extensions
     */
    @Override
    public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
        // Load query converters from other plugins
        List<QueryBuilderProtoConverter> extensions = loader.loadExtensions(QueryBuilderProtoConverter.class);
        if (extensions != null) {
            queryConverters.addAll(extensions);
        }
    }

    /**
     * Get the list of query converters, including those loaded from extensions.
     *
     * @return The list of query converters
     */
    public List<QueryBuilderProtoConverter> getQueryConverters() {
        return Collections.unmodifiableList(queryConverters);
    }

    /**
     * Get the query utils instance.
     *
     * @return The query utils instance
     * @throws IllegalStateException if queryUtils is not initialized
     */
    public AbstractQueryBuilderProtoUtils getQueryUtils() {
        if (queryUtils == null) {
            throw new IllegalStateException("Query utils not initialized. Make sure createComponents has been called.");
        }
        return queryUtils;
    }

    /**
     * Provides auxiliary transports for the plugin.
     * Creates and returns a map of transport names to transport suppliers.
     *
     * @param settings The node settings
     * @param threadPool The thread pool
     * @param circuitBreakerService The circuit breaker service
     * @param networkService The network service
     * @param clusterSettings The cluster settings
     * @param tracer The tracer
     * @return A map of transport names to transport suppliers
     * @throws IllegalStateException if queryRegistry is not initialized
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
        if (client == null) {
            throw new RuntimeException("client cannot be null");
        }

        if (queryRegistry == null) {
            throw new IllegalStateException("createComponents must be called before getAuxTransports to initialize the registry");
        }

        List<BindableService> grpcServices = registerGRPCServices(
            new DocumentServiceImpl(client),
            new SearchServiceImpl(client, queryUtils)
        );
        return Collections.singletonMap(
            GRPC_TRANSPORT_SETTING_KEY,
            () -> new Netty4GrpcServerTransport(settings, grpcServices, networkService)
        );
    }

    /**
     * Provides secure auxiliary transports for the plugin.
     * Registered under a distinct key from gRPC transport.
     * Consumes pluggable security settings as provided by a SecureAuxTransportSettingsProvider.
     *
     * @param settings The node settings
     * @param threadPool The thread pool
     * @param circuitBreakerService The circuit breaker service
     * @param networkService The network service
     * @param clusterSettings The cluster settings
     * @param tracer The tracer
     * @param secureAuxTransportSettingsProvider provides ssl context params
     * @return A map of transport names to transport suppliers
     * @throws IllegalStateException if queryRegistry is not initialized
     */
    @Override
    public Map<String, Supplier<AuxTransport>> getSecureAuxTransports(
        Settings settings,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        NetworkService networkService,
        ClusterSettings clusterSettings,
        SecureAuxTransportSettingsProvider secureAuxTransportSettingsProvider,
        Tracer tracer
    ) {
        if (client == null) {
            throw new RuntimeException("client cannot be null");
        }

        if (queryRegistry == null) {
            throw new IllegalStateException("createComponents must be called before getSecureAuxTransports to initialize the registry");
        }

        List<BindableService> grpcServices = registerGRPCServices(
            new DocumentServiceImpl(client),
            new SearchServiceImpl(client, queryUtils)
        );
        return Collections.singletonMap(
            GRPC_SECURE_TRANSPORT_SETTING_KEY,
            () -> new SecureNetty4GrpcServerTransport(settings, grpcServices, networkService, secureAuxTransportSettingsProvider)
        );
    }

    /**
     * Registers gRPC services to be exposed by the transport.
     *
     * @param services The gRPC services to register
     * @return A list of registered bindable services
     */
    private List<BindableService> registerGRPCServices(BindableService... services) {
        return List.of(services);
    }

    /**
     * Returns the settings defined by this plugin.
     *
     * @return A list of settings
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            SETTING_GRPC_PORT,
            SETTING_GRPC_PUBLISH_PORT,
            SETTING_GRPC_SECURE_PORT,
            SETTING_GRPC_HOST,
            SETTING_GRPC_PUBLISH_HOST,
            SETTING_GRPC_BIND_HOST,
            SETTING_GRPC_WORKER_COUNT,
            SETTING_GRPC_MAX_CONCURRENT_CONNECTION_CALLS,
            SETTING_GRPC_MAX_MSG_SIZE,
            SETTING_GRPC_MAX_CONNECTION_AGE,
            SETTING_GRPC_MAX_CONNECTION_IDLE,
            SETTING_GRPC_KEEPALIVE_TIMEOUT
        );
    }

    /**
     * Creates components used by the plugin.
     * Stores the client for later use in creating gRPC services, and the query registry which registers the types of supported GRPC Search queries.
     *
     * @param client The client
     * @param clusterService The cluster service
     * @param threadPool The thread pool
     * @param resourceWatcherService The resource watcher service
     * @param scriptService The script service
     * @param xContentRegistry The named content registry
     * @param environment The environment
     * @param nodeEnvironment The node environment
     * @param namedWriteableRegistry The named writeable registry
     * @param indexNameExpressionResolver The index name expression resolver
     * @param repositoriesServiceSupplier The repositories service supplier
     * @return A collection of components
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
        this.client = client;

        // Create the registry
        this.queryRegistry = new QueryBuilderProtoConverterRegistry();

        // Create the query utils instance
        this.queryUtils = new AbstractQueryBuilderProtoUtils(queryRegistry);

        // Register external converters
        for (QueryBuilderProtoConverter converter : queryConverters) {
            queryRegistry.registerConverter(converter);
        }

        return super.createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier
        );
    }
}
