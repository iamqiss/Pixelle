/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugins;

import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.lifecycle.LifecycleComponent;
import org.density.core.common.io.stream.NamedWriteable;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.env.NodeEnvironment;
import org.density.repositories.RepositoriesService;
import org.density.script.ScriptService;
import org.density.telemetry.metrics.MetricsRegistry;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.Client;
import org.density.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

/**
 * Plugin that provides the telemetry registries to build component with telemetry and also provide a way to
 * pass telemetry registries to the implementing plugins for adding instrumentation in the code.
 *
 * @density.experimental
 */
@ExperimentalApi
public interface TelemetryAwarePlugin {

    /**
     * Returns components added by this plugin.
     * <p>
     * Any components returned that implement {@link LifecycleComponent} will have their lifecycle managed.
     * Note: To aid in the migration away from guice, all objects returned as components will be bound in guice
     * to themselves.
     *
     * @param client A client to make requests to the system
     * @param clusterService A service to allow watching and updating cluster state
     * @param threadPool A service to allow retrieving an executor to run an async action
     * @param resourceWatcherService A service to watch for changes to node local files
     * @param scriptService A service to allow running scripts on the local node
     * @param xContentRegistry the registry for extensible xContent parsing
     * @param environment the environment for path and setting configurations
     * @param nodeEnvironment the node environment used coordinate access to the data paths
     * @param namedWriteableRegistry the registry for {@link NamedWriteable} object parsing
     * @param indexNameExpressionResolver A service that resolves expression to index and alias names
     * @param repositoriesServiceSupplier A supplier for the service that manages snapshot repositories; will return null when this method
     *                                   is called, but will return the repositories service once the node is initialized.
     * @param tracer the tracer to add tracing instrumentation.
     * @param metricsRegistry the registry for metrics instrumentation.
     */
    default Collection<Object> createComponents(
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
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        MetricsRegistry metricsRegistry
    ) {
        return Collections.emptyList();
    }
}
