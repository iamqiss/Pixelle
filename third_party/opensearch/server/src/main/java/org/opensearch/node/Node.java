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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.density.Build;
import org.density.ExceptionsHelper;
import org.density.DensityException;
import org.density.DensityParseException;
import org.density.DensityTimeoutException;
import org.density.Version;
import org.density.action.ActionModule;
import org.density.action.ActionModule.DynamicActionRegistry;
import org.density.action.ActionType;
import org.density.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.density.action.admin.indices.view.ViewService;
import org.density.action.search.SearchExecutionStatsCollector;
import org.density.action.search.SearchPhaseController;
import org.density.action.search.SearchRequestOperationsCompositeListenerFactory;
import org.density.action.search.SearchRequestOperationsListener;
import org.density.action.search.SearchRequestSlowLog;
import org.density.action.search.SearchRequestStats;
import org.density.action.search.SearchTaskRequestOperationsListener;
import org.density.action.search.SearchTransportService;
import org.density.action.search.StreamSearchTransportService;
import org.density.action.support.TransportAction;
import org.density.action.update.UpdateHelper;
import org.density.arrow.spi.StreamManager;
import org.density.bootstrap.BootstrapCheck;
import org.density.bootstrap.BootstrapContext;
import org.density.cluster.ClusterInfoService;
import org.density.cluster.ClusterManagerMetrics;
import org.density.cluster.ClusterModule;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.ClusterStateObserver;
import org.density.cluster.InternalClusterInfoService;
import org.density.cluster.NodeConnectionsService;
import org.density.cluster.StreamNodeConnectionsService;
import org.density.cluster.action.index.MappingUpdatedAction;
import org.density.cluster.action.shard.LocalShardStateAction;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.applicationtemplates.SystemTemplatesPlugin;
import org.density.cluster.applicationtemplates.SystemTemplatesService;
import org.density.cluster.coordination.PersistedStateRegistry;
import org.density.cluster.metadata.AliasValidator;
import org.density.cluster.metadata.IndexTemplateMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.MetadataCreateDataStreamService;
import org.density.cluster.metadata.MetadataCreateIndexService;
import org.density.cluster.metadata.MetadataIndexUpgradeService;
import org.density.cluster.metadata.SystemIndexMetadataUpgradeService;
import org.density.cluster.metadata.TemplateUpgradeService;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.routing.BatchedRerouteService;
import org.density.cluster.routing.RerouteService;
import org.density.cluster.routing.allocation.AwarenessReplicaBalance;
import org.density.cluster.routing.allocation.DiskThresholdMonitor;
import org.density.cluster.service.ClusterService;
import org.density.cluster.service.LocalClusterService;
import org.density.common.Nullable;
import org.density.common.SetOnce;
import org.density.common.StopWatch;
import org.density.common.cache.module.CacheModule;
import org.density.common.cache.service.CacheService;
import org.density.common.inject.Injector;
import org.density.common.inject.Key;
import org.density.common.inject.Module;
import org.density.common.inject.ModulesBuilder;
import org.density.common.inject.util.Providers;
import org.density.common.lease.Releasables;
import org.density.common.lifecycle.Lifecycle;
import org.density.common.lifecycle.LifecycleComponent;
import org.density.common.logging.DeprecationLogger;
import org.density.common.logging.HeaderWarning;
import org.density.common.logging.NodeAndClusterIdStateListener;
import org.density.common.network.NetworkAddress;
import org.density.common.network.NetworkModule;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.ConsistentSettingsService;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.settings.SettingUpgrader;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsException;
import org.density.common.settings.SettingsModule;
import org.density.common.unit.RatioValue;
import org.density.common.unit.TimeValue;
import org.density.common.util.BigArrays;
import org.density.common.util.FeatureFlags;
import org.density.common.util.PageCacheRecycler;
import org.density.common.util.io.IOUtils;
import org.density.core.Assertions;
import org.density.core.common.breaker.CircuitBreaker;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.transport.BoundTransportAddress;
import org.density.core.common.transport.TransportAddress;
import org.density.core.common.unit.ByteSizeUnit;
import org.density.core.common.unit.ByteSizeValue;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.crypto.CryptoHandlerRegistry;
import org.density.discovery.Discovery;
import org.density.discovery.DiscoveryModule;
import org.density.discovery.LocalDiscovery;
import org.density.env.Environment;
import org.density.env.NodeEnvironment;
import org.density.env.NodeMetadata;
import org.density.extensions.ExtensionsManager;
import org.density.extensions.NoopExtensionsManager;
import org.density.gateway.GatewayAllocator;
import org.density.gateway.GatewayMetaState;
import org.density.gateway.GatewayModule;
import org.density.gateway.GatewayService;
import org.density.gateway.MetaStateService;
import org.density.gateway.PersistedClusterStateService;
import org.density.gateway.ShardsBatchGatewayAllocator;
import org.density.gateway.remote.RemoteClusterStateCleanupManager;
import org.density.gateway.remote.RemoteClusterStateService;
import org.density.http.HttpServerTransport;
import org.density.identity.IdentityService;
import org.density.index.IndexModule;
import org.density.index.IndexSettings;
import org.density.index.IndexingPressureService;
import org.density.index.IngestionConsumerFactory;
import org.density.index.SegmentReplicationStatsTracker;
import org.density.index.analysis.AnalysisRegistry;
import org.density.index.autoforcemerge.AutoForceMergeManager;
import org.density.index.compositeindex.CompositeIndexSettings;
import org.density.index.engine.EngineFactory;
import org.density.index.engine.MergedSegmentWarmerFactory;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.index.recovery.RemoteStoreRestoreService;
import org.density.index.remote.RemoteIndexPathUploader;
import org.density.index.remote.RemoteStoreStatsTrackerFactory;
import org.density.index.store.DefaultCompositeDirectoryFactory;
import org.density.index.store.IndexStoreListener;
import org.density.index.store.RemoteSegmentStoreDirectoryFactory;
import org.density.index.store.remote.filecache.FileCache;
import org.density.index.store.remote.filecache.FileCacheCleaner;
import org.density.index.store.remote.filecache.FileCacheFactory;
import org.density.index.store.remote.filecache.FileCacheSettings;
import org.density.indices.IndicesModule;
import org.density.indices.IndicesService;
import org.density.indices.RemoteStoreSettings;
import org.density.indices.ShardLimitValidator;
import org.density.indices.SystemIndexDescriptor;
import org.density.indices.SystemIndices;
import org.density.indices.analysis.AnalysisModule;
import org.density.indices.breaker.BreakerSettings;
import org.density.indices.breaker.HierarchyCircuitBreakerService;
import org.density.indices.cluster.IndicesClusterStateService;
import org.density.indices.recovery.PeerRecoverySourceService;
import org.density.indices.recovery.PeerRecoveryTargetService;
import org.density.indices.recovery.RecoverySettings;
import org.density.indices.replication.SegmentReplicationSourceFactory;
import org.density.indices.replication.SegmentReplicationSourceService;
import org.density.indices.replication.SegmentReplicationTargetService;
import org.density.indices.replication.SegmentReplicator;
import org.density.indices.replication.checkpoint.MergedSegmentPublisher;
import org.density.indices.replication.checkpoint.PublishMergedSegmentAction;
import org.density.indices.replication.checkpoint.RemoteStorePublishMergedSegmentAction;
import org.density.indices.store.IndicesStore;
import org.density.ingest.IngestService;
import org.density.ingest.SystemIngestPipelineCache;
import org.density.monitor.MonitorService;
import org.density.monitor.fs.FsHealthService;
import org.density.monitor.fs.FsProbe;
import org.density.monitor.fs.FsServiceProvider;
import org.density.monitor.jvm.JvmInfo;
import org.density.node.remotestore.RemoteStoreNodeService;
import org.density.node.remotestore.RemoteStorePinnedTimestampService;
import org.density.node.resource.tracker.NodeResourceUsageTracker;
import org.density.persistent.PersistentTasksClusterService;
import org.density.persistent.PersistentTasksExecutor;
import org.density.persistent.PersistentTasksExecutorRegistry;
import org.density.persistent.PersistentTasksService;
import org.density.plugins.ActionPlugin;
import org.density.plugins.AnalysisPlugin;
import org.density.plugins.CachePlugin;
import org.density.plugins.CircuitBreakerPlugin;
import org.density.plugins.ClusterPlugin;
import org.density.plugins.CryptoKeyProviderPlugin;
import org.density.plugins.CryptoPlugin;
import org.density.plugins.DiscoveryPlugin;
import org.density.plugins.EnginePlugin;
import org.density.plugins.ExtensionAwarePlugin;
import org.density.plugins.IdentityAwarePlugin;
import org.density.plugins.IdentityPlugin;
import org.density.plugins.IndexStorePlugin;
import org.density.plugins.IngestPlugin;
import org.density.plugins.IngestionConsumerPlugin;
import org.density.plugins.MapperPlugin;
import org.density.plugins.MetadataUpgrader;
import org.density.plugins.NetworkPlugin;
import org.density.plugins.PersistentTaskPlugin;
import org.density.plugins.Plugin;
import org.density.plugins.PluginInfo;
import org.density.plugins.PluginsService;
import org.density.plugins.RepositoryPlugin;
import org.density.plugins.ScriptPlugin;
import org.density.plugins.SearchPipelinePlugin;
import org.density.plugins.SearchPlugin;
import org.density.plugins.SecureSettingsFactory;
import org.density.plugins.StreamManagerPlugin;
import org.density.plugins.SystemIndexPlugin;
import org.density.plugins.TaskManagerClientPlugin;
import org.density.plugins.TelemetryAwarePlugin;
import org.density.plugins.TelemetryPlugin;
import org.density.ratelimitting.admissioncontrol.AdmissionControlService;
import org.density.ratelimitting.admissioncontrol.transport.AdmissionControlTransportInterceptor;
import org.density.repositories.RepositoriesModule;
import org.density.repositories.RepositoriesService;
import org.density.rest.RestController;
import org.density.script.ScriptContext;
import org.density.script.ScriptEngine;
import org.density.script.ScriptModule;
import org.density.script.ScriptService;
import org.density.search.SearchModule;
import org.density.search.SearchService;
import org.density.search.aggregations.support.AggregationUsageService;
import org.density.search.backpressure.SearchBackpressureService;
import org.density.search.backpressure.settings.SearchBackpressureSettings;
import org.density.search.deciders.ConcurrentSearchRequestDecider;
import org.density.search.fetch.FetchPhase;
import org.density.search.pipeline.SearchPipelineService;
import org.density.search.query.QueryPhase;
import org.density.snapshots.InternalSnapshotsInfoService;
import org.density.snapshots.RestoreService;
import org.density.snapshots.SnapshotShardsService;
import org.density.snapshots.SnapshotsInfoService;
import org.density.snapshots.SnapshotsService;
import org.density.task.commons.clients.TaskManagerClient;
import org.density.tasks.Task;
import org.density.tasks.TaskCancellationMonitoringService;
import org.density.tasks.TaskCancellationMonitoringSettings;
import org.density.tasks.TaskCancellationService;
import org.density.tasks.TaskResourceTrackingService;
import org.density.tasks.TaskResultsService;
import org.density.tasks.consumer.TopNSearchTasksLogger;
import org.density.telemetry.TelemetryModule;
import org.density.telemetry.TelemetrySettings;
import org.density.telemetry.metrics.MetricsRegistry;
import org.density.telemetry.metrics.MetricsRegistryFactory;
import org.density.telemetry.metrics.NoopMetricsRegistryFactory;
import org.density.telemetry.tracing.NoopTracerFactory;
import org.density.telemetry.tracing.Tracer;
import org.density.telemetry.tracing.TracerFactory;
import org.density.threadpool.ExecutorBuilder;
import org.density.threadpool.RunnableTaskExecutionListener;
import org.density.threadpool.ThreadPool;
import org.density.transport.AuxTransport;
import org.density.transport.RemoteClusterService;
import org.density.transport.StreamTransportService;
import org.density.transport.Transport;
import org.density.transport.TransportInterceptor;
import org.density.transport.TransportService;
import org.density.transport.client.Client;
import org.density.transport.client.node.NodeClient;
import org.density.usage.UsageService;
import org.density.watcher.ResourceWatcherService;
import org.density.wlm.WorkloadGroupService;
import org.density.wlm.WorkloadGroupsStateAccessor;
import org.density.wlm.WorkloadManagementSettings;
import org.density.wlm.WorkloadManagementTransportInterceptor;
import org.density.wlm.cancellation.MaximumResourceTaskSelectionStrategy;
import org.density.wlm.cancellation.WorkloadGroupTaskCancellationService;
import org.density.wlm.listeners.WorkloadGroupRequestOperationListener;
import org.density.wlm.tracker.WorkloadGroupResourceUsageTrackerService;

import javax.net.ssl.SNIHostName;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.density.common.util.FeatureFlags.ARROW_STREAMS_SETTING;
import static org.density.common.util.FeatureFlags.BACKGROUND_TASK_EXECUTION_EXPERIMENTAL;
import static org.density.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.density.common.util.FeatureFlags.TELEMETRY;
import static org.density.index.ShardIndexingPressureSettings.SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY;
import static org.density.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.isRemoteClusterStateConfigured;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.isRemoteDataAttributePresent;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreAttributePresent;

/**
 * A node represent a node within a cluster ({@code cluster.name}). The {@link #client()} can be used
 * in order to use a {@link Client} to perform actions/operations against the cluster.
 *
 * @density.internal
 */
public class Node implements Closeable {
    public static final Setting<Boolean> WRITE_PORTS_FILE_SETTING = Setting.boolSetting("node.portsfile", false, Property.NodeScope);
    private static final Setting<Boolean> NODE_DATA_SETTING = Setting.boolSetting(
        "node.data",
        true,
        Property.Deprecated,
        Property.NodeScope
    );
    private static final Setting<Boolean> NODE_MASTER_SETTING = Setting.boolSetting(
        "node.master",
        true,
        Property.Deprecated,
        Property.NodeScope
    );
    private static final Setting<Boolean> NODE_INGEST_SETTING = Setting.boolSetting(
        "node.ingest",
        true,
        Property.Deprecated,
        Property.NodeScope
    );
    private static final Setting<Boolean> NODE_REMOTE_CLUSTER_CLIENT = Setting.boolSetting(
        "node.remote_cluster_client",
        RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
        Property.Deprecated,
        Property.NodeScope
    );

    /**
     * controls whether the node is allowed to persist things like metadata to disk
     * Note that this does not control whether the node stores actual indices (see
     * {@link #NODE_DATA_SETTING}). However, if this is false, {@link #NODE_DATA_SETTING}
     * and {@link #NODE_MASTER_SETTING} must also be false.
     */
    public static final Setting<Boolean> NODE_LOCAL_STORAGE_SETTING = Setting.boolSetting(
        "node.local_storage",
        true,
        Property.Deprecated,
        Property.NodeScope
    );
    public static final Setting<String> NODE_NAME_SETTING = Setting.simpleString("node.name", Property.NodeScope);
    public static final Setting.AffixSetting<String> NODE_ATTRIBUTES = Setting.prefixKeySetting(
        "node.attr.",
        (key) -> new Setting<>(key, "", (value) -> {
            if (value.length() > 0
                && (Character.isWhitespace(value.charAt(0)) || Character.isWhitespace(value.charAt(value.length() - 1)))) {
                throw new IllegalArgumentException(key + " cannot have leading or trailing whitespace " + "[" + value + "]");
            }
            if (value.length() > 0 && "node.attr.server_name".equals(key)) {
                try {
                    new SNIHostName(value);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("invalid node.attr.server_name [" + value + "]", e);
                }
            }
            return value;
        }, Property.NodeScope)
    );
    public static final Setting<String> BREAKER_TYPE_KEY = new Setting<>("indices.breaker.type", "hierarchy", (s) -> {
        switch (s) {
            case "hierarchy":
            case "none":
                return s;
            default:
                throw new IllegalArgumentException("indices.breaker.type must be one of [hierarchy, none] but was: " + s);
        }
    }, Setting.Property.NodeScope);

    private static final String ZERO = "0";

    public static final Setting<String> NODE_SEARCH_CACHE_SIZE_SETTING = new Setting<>(
        "node.search.cache.size",
        s -> (DiscoveryNode.isDedicatedWarmNode(s)) ? "80%" : ZERO,
        Node::validateFileCacheSize,
        Property.NodeScope
    );

    private static final String CLIENT_TYPE = "node";

    /**
     * The discovery settings for the node.
     *
     * @density.internal
     */
    public static class DiscoverySettings {
        public static final Setting<TimeValue> INITIAL_STATE_TIMEOUT_SETTING = Setting.positiveTimeSetting(
            "discovery.initial_state_timeout",
            TimeValue.timeValueSeconds(30),
            Property.NodeScope
        );
    }

    private final Lifecycle lifecycle = new Lifecycle();

    /**
     * This logger instance is an instance field as opposed to a static field. This ensures that the field is not
     * initialized until an instance of Node is constructed, which is sure to happen after the logging infrastructure
     * has been initialized to include the hostname. If this field were static, then it would be initialized when the
     * class initializer runs. Alas, this happens too early, before logging is initialized as this class is referred to
     * in InternalSettingsPreparer#finalizeSettings, which runs when creating the Environment, before logging is
     * initialized.
     */
    private final Logger logger = LogManager.getLogger(Node.class);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(Node.class);
    private final Injector injector;
    private final Environment environment;
    private final NodeEnvironment nodeEnvironment;
    private final PluginsService pluginsService;
    private final ExtensionsManager extensionsManager;
    private final NodeClient client;
    private final Collection<LifecycleComponent> pluginLifecycleComponents;
    private final LocalNodeFactory localNodeFactory;
    private final NodeService nodeService;
    private final Tracer tracer;
    private final AutoForceMergeManager autoForceMergeManager;
    private final MetricsRegistry metricsRegistry;
    final NamedWriteableRegistry namedWriteableRegistry;
    private final AtomicReference<RunnableTaskExecutionListener> runnableTaskListener;
    private FileCache fileCache;
    private final RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;
    private final MergedSegmentWarmerFactory mergedSegmentWarmerFactory;

    public Node(Environment environment) {
        this(environment, Collections.emptyList(), true);
    }

    /**
     * Constructs a node
     *
     * @param initialEnvironment         the initial environment for this node, which will be added to by plugins
     * @param classpathPlugins           the plugins to be loaded from the classpath
     * @param forbidPrivateIndexSettings whether or not private index settings are forbidden when creating an index; this is used in the
     *                                   test framework for tests that rely on being able to set private settings
     */
    protected Node(final Environment initialEnvironment, Collection<PluginInfo> classpathPlugins, boolean forbidPrivateIndexSettings) {
        final List<Closeable> resourcesToClose = new ArrayList<>(); // register everything we need to release in the case of an error
        boolean success = false;
        try {
            Settings tmpSettings = Settings.builder()
                .put(initialEnvironment.settings())
                .put(Client.CLIENT_TYPE_SETTING_S.getKey(), CLIENT_TYPE)
                // Enabling shard indexing backpressure node-attribute
                .put(NODE_ATTRIBUTES.getKey() + SHARD_INDEXING_PRESSURE_ENABLED_ATTRIBUTE_KEY, "true")
                .build();

            final JvmInfo jvmInfo = JvmInfo.jvmInfo();
            logger.info(
                "version[{}], pid[{}], build[{}/{}/{}], OS[{}/{}/{}], JVM[{}/{}/{}/{}]",
                Build.CURRENT.getQualifiedVersion(),
                jvmInfo.pid(),
                Build.CURRENT.type().displayName(),
                Build.CURRENT.hash(),
                Build.CURRENT.date(),
                Constants.OS_NAME,
                Constants.OS_VERSION,
                Constants.OS_ARCH,
                Constants.JVM_VENDOR,
                Constants.JVM_NAME,
                System.getProperty("java.version"),
                Runtime.version().toString()
            );
            if (jvmInfo.getBundledJdk()) {
                logger.info("JVM home [{}], using bundled JDK/JRE [{}]", System.getProperty("java.home"), jvmInfo.getUsingBundledJdk());
            } else {
                logger.info("JVM home [{}]", System.getProperty("java.home"));
                deprecationLogger.deprecate(
                    "no-jdk",
                    "no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release"
                );
            }
            logger.info("JVM arguments {}", Arrays.toString(jvmInfo.getInputArguments()));
            if (Build.CURRENT.isProductionRelease() == false) {
                logger.warn(
                    "version [{}] is a pre-release version of Density and is not suitable for production",
                    Build.CURRENT.getQualifiedVersion()
                );
            }

            if (logger.isDebugEnabled()) {
                logger.debug(
                    "using config [{}], data [{}], logs [{}], plugins [{}]",
                    initialEnvironment.configDir(),
                    Arrays.toString(initialEnvironment.dataFiles()),
                    initialEnvironment.logsDir(),
                    initialEnvironment.pluginsDir()
                );
            }

            // Ensure feature flags from density.yml are valid during plugin initialization.
            FeatureFlags.initializeFeatureFlags(tmpSettings);

            this.pluginsService = new PluginsService(
                tmpSettings,
                initialEnvironment.configDir(),
                initialEnvironment.modulesDir(),
                initialEnvironment.pluginsDir(),
                classpathPlugins
            );

            final Settings settings = pluginsService.updatedSettings();

            final List<IdentityPlugin> identityPlugins = new ArrayList<>();
            identityPlugins.addAll(pluginsService.filterPlugins(IdentityPlugin.class));

            final Set<DiscoveryNodeRole> additionalRoles = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .map(Plugin::getRoles)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
            DiscoveryNode.setAdditionalRoles(additionalRoles);

            DiscoveryNode.setDeprecatedMasterRole();

            /*
             * Create the environment based on the finalized view of the settings. This is to ensure that components get the same setting
             * values, no matter they ask for them from.
             */
            this.environment = new Environment(settings, initialEnvironment.configDir(), Node.NODE_LOCAL_STORAGE_SETTING.get(settings));
            Environment.assertEquivalent(initialEnvironment, this.environment);
            Stream<IndexStoreListener> indexStoreListenerStream = pluginsService.filterPlugins(IndexStorePlugin.class)
                .stream()
                .map(IndexStorePlugin::getIndexStoreListener)
                .filter(Optional::isPresent)
                .map(Optional::get);
            // FileCache is only initialized on warm nodes, so we only create FileCacheCleaner on warm nodes as well
            if (DiscoveryNode.isWarmNode(settings) == false) {
                nodeEnvironment = new NodeEnvironment(
                    settings,
                    environment,
                    new IndexStoreListener.CompositeIndexStoreListener(indexStoreListenerStream.collect(Collectors.toList()))
                );
            } else {
                nodeEnvironment = new NodeEnvironment(
                    settings,
                    environment,
                    new IndexStoreListener.CompositeIndexStoreListener(
                        Stream.concat(indexStoreListenerStream, Stream.of(new FileCacheCleaner(this::fileCache)))
                            .collect(Collectors.toList())
                    )
                );
            }
            logger.info(
                "node name [{}], node ID [{}], cluster name [{}], roles {}",
                NODE_NAME_SETTING.get(tmpSettings),
                nodeEnvironment.nodeId(),
                ClusterName.CLUSTER_NAME_SETTING.get(tmpSettings).value(),
                DiscoveryNode.getRolesFromSettings(settings)
                    .stream()
                    .map(DiscoveryNodeRole::roleName)
                    .collect(Collectors.toCollection(LinkedHashSet::new))
            );
            resourcesToClose.add(nodeEnvironment);

            final List<ExecutorBuilder<?>> executorBuilders = pluginsService.getExecutorBuilders(settings);

            runnableTaskListener = new AtomicReference<>();
            final ThreadPool threadPool = new ThreadPool(settings, runnableTaskListener, executorBuilders.toArray(new ExecutorBuilder[0]));

            final IdentityService identityService = new IdentityService(settings, threadPool, identityPlugins);

            if (FeatureFlags.isEnabled(FeatureFlags.EXTENSIONS)) {
                final List<ExtensionAwarePlugin> extensionAwarePlugins = pluginsService.filterPlugins(ExtensionAwarePlugin.class);
                Set<Setting<?>> additionalSettings = new HashSet<>();
                for (ExtensionAwarePlugin extAwarePlugin : extensionAwarePlugins) {
                    additionalSettings.addAll(extAwarePlugin.getExtensionSettings());
                }
                this.extensionsManager = new ExtensionsManager(additionalSettings, identityService);
            } else {
                this.extensionsManager = new NoopExtensionsManager(identityService);
            }

            final SetOnce<RepositoriesService> repositoriesServiceReference = new SetOnce<>();
            final RemoteStoreNodeService remoteStoreNodeService = new RemoteStoreNodeService(repositoriesServiceReference::get, threadPool);
            localNodeFactory = new LocalNodeFactory(settings, nodeEnvironment.nodeId(), remoteStoreNodeService);
            resourcesToClose.add(() -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS));
            final ResourceWatcherService resourceWatcherService = new ResourceWatcherService(settings, threadPool);
            resourcesToClose.add(resourceWatcherService);
            // adds the context to the DeprecationLogger so that it does not need to be injected everywhere
            HeaderWarning.setThreadContext(threadPool.getThreadContext());
            resourcesToClose.add(() -> HeaderWarning.removeThreadContext(threadPool.getThreadContext()));

            final List<Setting<?>> additionalSettings = new ArrayList<>();
            // register the node.data, node.ingest, node.master, node.remote_cluster_client settings here so we can mark them private
            additionalSettings.add(NODE_DATA_SETTING);
            additionalSettings.add(NODE_INGEST_SETTING);
            additionalSettings.add(NODE_MASTER_SETTING);
            additionalSettings.add(NODE_REMOTE_CLUSTER_CLIENT);
            additionalSettings.addAll(pluginsService.getPluginSettings());
            final List<String> additionalSettingsFilter = new ArrayList<>(pluginsService.getPluginSettingsFilter());
            for (final ExecutorBuilder<?> builder : threadPool.builders()) {
                additionalSettings.addAll(builder.getRegisteredSettings());
            }
            client = new NodeClient(settings, threadPool);

            final ScriptModule scriptModule = new ScriptModule(settings, pluginsService.filterPlugins(ScriptPlugin.class));
            final ScriptService scriptService = newScriptService(settings, scriptModule.engines, scriptModule.contexts);
            AnalysisModule analysisModule = new AnalysisModule(this.environment, pluginsService.filterPlugins(AnalysisPlugin.class));
            // this is as early as we can validate settings at this point. we already pass them to ScriptModule as well as ThreadPool
            // so we might be late here already

            final Set<SettingUpgrader<?>> settingsUpgraders = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .map(Plugin::getSettingUpgraders)
                .flatMap(List::stream)
                .collect(Collectors.toSet());

            final SettingsModule settingsModule = new SettingsModule(
                settings,
                additionalSettings,
                additionalSettingsFilter,
                settingsUpgraders
            );
            threadPool.registerClusterSettingsListeners(settingsModule.getClusterSettings());
            scriptModule.registerClusterSettingsListeners(scriptService, settingsModule.getClusterSettings());
            final NetworkService networkService = new NetworkService(
                getCustomNameResolvers(pluginsService.filterPlugins(DiscoveryPlugin.class))
            );

            TracerFactory tracerFactory;
            MetricsRegistryFactory metricsRegistryFactory;
            if (FeatureFlags.isEnabled(TELEMETRY)) {
                final TelemetrySettings telemetrySettings = new TelemetrySettings(settings, settingsModule.getClusterSettings());
                if (telemetrySettings.isTracingFeatureEnabled() || telemetrySettings.isMetricsFeatureEnabled()) {
                    List<TelemetryPlugin> telemetryPlugins = pluginsService.filterPlugins(TelemetryPlugin.class);
                    List<TelemetryPlugin> telemetryPluginsImplementingTelemetryAware = telemetryPlugins.stream()
                        .filter(a -> TelemetryAwarePlugin.class.isAssignableFrom(a.getClass()))
                        .collect(toList());
                    if (telemetryPluginsImplementingTelemetryAware.isEmpty() == false) {
                        throw new IllegalStateException(
                            String.format(
                                Locale.ROOT,
                                "Telemetry plugins %s should not implement TelemetryAwarePlugin interface",
                                telemetryPluginsImplementingTelemetryAware
                            )
                        );
                    }
                    TelemetryModule telemetryModule = new TelemetryModule(telemetryPlugins, telemetrySettings);
                    if (telemetrySettings.isTracingFeatureEnabled()) {
                        tracerFactory = new TracerFactory(telemetrySettings, telemetryModule.getTelemetry(), threadPool.getThreadContext());
                    } else {
                        tracerFactory = new NoopTracerFactory();
                    }
                    if (telemetrySettings.isMetricsFeatureEnabled()) {
                        metricsRegistryFactory = new MetricsRegistryFactory(telemetrySettings, telemetryModule.getTelemetry());
                    } else {
                        metricsRegistryFactory = new NoopMetricsRegistryFactory();
                    }
                } else {
                    tracerFactory = new NoopTracerFactory();
                    metricsRegistryFactory = new NoopMetricsRegistryFactory();
                }
            } else {
                tracerFactory = new NoopTracerFactory();
                metricsRegistryFactory = new NoopMetricsRegistryFactory();
            }

            tracer = tracerFactory.getTracer();
            metricsRegistry = metricsRegistryFactory.getMetricsRegistry();
            resourcesToClose.add(tracer::close);
            resourcesToClose.add(metricsRegistry::close);

            final ClusterManagerMetrics clusterManagerMetrics = new ClusterManagerMetrics(metricsRegistry);

            List<ClusterPlugin> clusterPlugins = pluginsService.filterPlugins(ClusterPlugin.class);
            final boolean clusterless = clusterPlugins.stream().anyMatch(ClusterPlugin::isClusterless);
            final ClusterService clusterService;
            if (clusterless) {
                clusterService = new LocalClusterService(settings, settingsModule.getClusterSettings(), threadPool, clusterManagerMetrics);
            } else {
                clusterService = new ClusterService(settings, settingsModule.getClusterSettings(), threadPool, clusterManagerMetrics);
            }
            clusterService.addStateApplier(scriptService);
            resourcesToClose.add(clusterService);
            final Set<Setting<?>> consistentSettings = settingsModule.getConsistentSettings();
            if (consistentSettings.isEmpty() == false) {
                clusterService.addLocalNodeClusterManagerListener(
                    new ConsistentSettingsService(settings, clusterService, consistentSettings).newHashPublisher()
                );
            }

            SystemTemplatesService systemTemplatesService = new SystemTemplatesService(
                pluginsService.filterPlugins(SystemTemplatesPlugin.class),
                threadPool,
                clusterService.getClusterSettings(),
                settings
            );
            systemTemplatesService.verifyRepositories();
            clusterService.addLocalNodeClusterManagerListener(systemTemplatesService);

            final ClusterInfoService clusterInfoService = newClusterInfoService(settings, clusterService, threadPool, client);
            final UsageService usageService = new UsageService();

            ModulesBuilder modules = new ModulesBuilder();
            // plugin modules must be added here, before others or we can get crazy injection errors...
            for (Module pluginModule : pluginsService.createGuiceModules()) {
                modules.add(pluginModule);
            }
            final FsHealthService fsHealthService = new FsHealthService(
                settings,
                clusterService.getClusterSettings(),
                threadPool,
                nodeEnvironment,
                metricsRegistry
            );
            final SetOnce<RerouteService> rerouteServiceReference = new SetOnce<>();
            final InternalSnapshotsInfoService snapshotsInfoService = new InternalSnapshotsInfoService(
                settings,
                clusterService,
                repositoriesServiceReference::get,
                rerouteServiceReference::get
            );
            final Map<String, Collection<SystemIndexDescriptor>> systemIndexDescriptorMap = Collections.unmodifiableMap(
                pluginsService.filterPlugins(SystemIndexPlugin.class)
                    .stream()
                    .collect(
                        Collectors.toMap(
                            plugin -> plugin.getClass().getCanonicalName(),
                            plugin -> plugin.getSystemIndexDescriptors(settings)
                        )
                    )
            );
            final SystemIndices systemIndices = new SystemIndices(systemIndexDescriptorMap);
            final ClusterModule clusterModule = new ClusterModule(
                settings,
                clusterService,
                clusterPlugins,
                clusterInfoService,
                snapshotsInfoService,
                threadPool.getThreadContext(),
                clusterManagerMetrics,
                clusterless ? LocalShardStateAction.class : ShardStateAction.class
            );
            modules.add(clusterModule);
            final List<MapperPlugin> mapperPlugins = pluginsService.filterPlugins(MapperPlugin.class);
            IndicesModule indicesModule = new IndicesModule(mapperPlugins);
            modules.add(indicesModule);

            SearchModule searchModule = new SearchModule(settings, pluginsService.filterPlugins(SearchPlugin.class));
            List<BreakerSettings> pluginCircuitBreakers = pluginsService.filterPlugins(CircuitBreakerPlugin.class)
                .stream()
                .map(plugin -> plugin.getCircuitBreaker(settings))
                .collect(Collectors.toList());
            final CircuitBreakerService circuitBreakerService = createCircuitBreakerService(
                settingsModule.getSettings(),
                pluginCircuitBreakers,
                settingsModule.getClusterSettings()
            );
            // File cache will be initialized by the node once circuit breakers are in place.
            initializeFileCache(settings, circuitBreakerService.getBreaker(CircuitBreaker.REQUEST));

            pluginsService.filterPlugins(CircuitBreakerPlugin.class).forEach(plugin -> {
                CircuitBreaker breaker = circuitBreakerService.getBreaker(plugin.getCircuitBreaker(settings).getName());
                plugin.setCircuitBreaker(breaker);
            });
            resourcesToClose.add(circuitBreakerService);
            modules.add(new GatewayModule());

            PageCacheRecycler pageCacheRecycler = createPageCacheRecycler(settings);
            BigArrays bigArrays = createBigArrays(pageCacheRecycler, circuitBreakerService);
            modules.add(settingsModule);
            List<NamedWriteableRegistry.Entry> namedWriteables = Stream.of(
                NetworkModule.getNamedWriteables().stream(),
                indicesModule.getNamedWriteables().stream(),
                searchModule.getNamedWriteables().stream(),
                pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getNamedWriteables().stream()),
                ClusterModule.getNamedWriteables().stream()
            ).flatMap(Function.identity()).collect(Collectors.toList());
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
            NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
                Stream.of(
                    NetworkModule.getNamedXContents().stream(),
                    IndicesModule.getNamedXContents().stream(),
                    searchModule.getNamedXContents().stream(),
                    pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getNamedXContent().stream()),
                    ClusterModule.getNamedXWriteables().stream()
                ).flatMap(Function.identity()).collect(toList())
            );
            final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry);
            final PersistedClusterStateService lucenePersistedStateFactory = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry,
                bigArrays,
                clusterService.getClusterSettings(),
                threadPool::relativeTimeInMillis
            );
            final RemoteStoreSettings remoteStoreSettings = new RemoteStoreSettings(settings, settingsModule.getClusterSettings());
            final RemoteClusterStateService remoteClusterStateService;
            final RemoteClusterStateCleanupManager remoteClusterStateCleanupManager;
            final RemoteIndexPathUploader remoteIndexPathUploader;
            if (isRemoteClusterStateConfigured(settings)) {
                remoteIndexPathUploader = new RemoteIndexPathUploader(
                    threadPool,
                    settings,
                    repositoriesServiceReference::get,
                    clusterService.getClusterSettings(),
                    remoteStoreSettings
                );
                remoteClusterStateService = new RemoteClusterStateService(
                    nodeEnvironment.nodeId(),
                    repositoriesServiceReference::get,
                    settings,
                    clusterService,
                    threadPool::preciseRelativeTimeInNanos,
                    threadPool,
                    List.of(remoteIndexPathUploader),
                    namedWriteableRegistry
                );
                remoteClusterStateCleanupManager = remoteClusterStateService.getCleanupManager();
            } else {
                remoteClusterStateService = null;
                remoteIndexPathUploader = null;
                remoteClusterStateCleanupManager = null;
            }
            final RemoteStorePinnedTimestampService remoteStorePinnedTimestampService;
            if (isRemoteDataAttributePresent(settings) && CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.get(settings)) {
                remoteStorePinnedTimestampService = new RemoteStorePinnedTimestampService(
                    repositoriesServiceReference::get,
                    settings,
                    threadPool,
                    clusterService
                );
                resourcesToClose.add(remoteStorePinnedTimestampService);
            } else {
                remoteStorePinnedTimestampService = null;
            }

            // collect engine factory providers from plugins
            final Collection<EnginePlugin> enginePlugins = pluginsService.filterPlugins(EnginePlugin.class);
            final Collection<Function<IndexSettings, Optional<EngineFactory>>> engineFactoryProviders = enginePlugins.stream()
                .map(plugin -> (Function<IndexSettings, Optional<EngineFactory>>) plugin::getEngineFactory)
                .collect(Collectors.toList());

            // collect ingestion consumer factory providers from plugins
            final Map<String, IngestionConsumerFactory> ingestionConsumerFactories = new HashMap<>();
            pluginsService.filterPlugins(IngestionConsumerPlugin.class)
                .forEach(plugin -> ingestionConsumerFactories.putAll(plugin.getIngestionConsumerFactories()));

            final Map<String, IndexStorePlugin.DirectoryFactory> builtInDirectoryFactories = IndexModule.createBuiltInDirectoryFactories(
                repositoriesServiceReference::get,
                threadPool,
                fileCache
            );

            final Map<String, IndexStorePlugin.DirectoryFactory> directoryFactories = new HashMap<>();
            pluginsService.filterPlugins(IndexStorePlugin.class)
                .stream()
                .map(IndexStorePlugin::getDirectoryFactories)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                .forEach((k, v) -> {
                    // do not allow any plugin-provided index store type to conflict with a built-in type
                    if (builtInDirectoryFactories.containsKey(k)) {
                        throw new IllegalStateException("registered index store type [" + k + "] conflicts with a built-in type");
                    }
                    directoryFactories.put(k, v);
                });
            directoryFactories.putAll(builtInDirectoryFactories);

            final Map<String, IndexStorePlugin.CompositeDirectoryFactory> compositeDirectoryFactories = new HashMap<>();
            pluginsService.filterPlugins(IndexStorePlugin.class)
                .stream()
                .map(IndexStorePlugin::getCompositeDirectoryFactories)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                .forEach((k, v) -> {
                    if (k.equals("default")) {
                        throw new IllegalStateException(
                            "registered composite index store type [" + k + "] conflicts with a built-in default type"
                        );
                    }
                    compositeDirectoryFactories.put(k, v);
                });
            compositeDirectoryFactories.put("default", new DefaultCompositeDirectoryFactory());

            final Map<String, IndexStorePlugin.RecoveryStateFactory> recoveryStateFactories = pluginsService.filterPlugins(
                IndexStorePlugin.class
            )
                .stream()
                .map(IndexStorePlugin::getRecoveryStateFactories)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final Map<String, IndexStorePlugin.StoreFactory> storeFactories = pluginsService.filterPlugins(IndexStorePlugin.class)
                .stream()
                .map(IndexStorePlugin::getStoreFactories)
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            final RerouteService rerouteService = new BatchedRerouteService(clusterService, clusterModule.getAllocationService()::reroute);
            rerouteServiceReference.set(rerouteService);
            clusterService.setRerouteService(rerouteService);
            clusterModule.setRerouteServiceForAllocator(rerouteService);

            final RecoverySettings recoverySettings = new RecoverySettings(settings, settingsModule.getClusterSettings());

            final CompositeIndexSettings compositeIndexSettings = new CompositeIndexSettings(settings, settingsModule.getClusterSettings());

            final IndexStorePlugin.DirectoryFactory remoteDirectoryFactory = new RemoteSegmentStoreDirectoryFactory(
                repositoriesServiceReference::get,
                threadPool,
                remoteStoreSettings.getSegmentsPathFixedPrefix()
            );

            final TaskResourceTrackingService taskResourceTrackingService = new TaskResourceTrackingService(
                settings,
                clusterService.getClusterSettings(),
                threadPool
            );

            final SearchRequestStats searchRequestStats = new SearchRequestStats(clusterService.getClusterSettings());
            final SearchRequestSlowLog searchRequestSlowLog = new SearchRequestSlowLog(clusterService);
            final SearchTaskRequestOperationsListener searchTaskRequestOperationsListener = new SearchTaskRequestOperationsListener(
                taskResourceTrackingService
            );

            remoteStoreStatsTrackerFactory = new RemoteStoreStatsTrackerFactory(clusterService, settings);
            CacheModule cacheModule = new CacheModule(pluginsService.filterPlugins(CachePlugin.class), settings);
            CacheService cacheService = cacheModule.getCacheService();
            final SegmentReplicator segmentReplicator = new SegmentReplicator(threadPool);
            final IndicesService indicesService = new IndicesService(
                settings,
                pluginsService,
                nodeEnvironment,
                xContentRegistry,
                analysisModule.getAnalysisRegistry(),
                clusterModule.getIndexNameExpressionResolver(),
                indicesModule.getMapperRegistry(),
                namedWriteableRegistry,
                threadPool,
                settingsModule.getIndexScopedSettings(),
                circuitBreakerService,
                bigArrays,
                scriptService,
                clusterService,
                client,
                metaStateService,
                engineFactoryProviders,
                Map.copyOf(directoryFactories),
                Map.copyOf(compositeDirectoryFactories),
                searchModule.getValuesSourceRegistry(),
                recoveryStateFactories,
                storeFactories,
                remoteDirectoryFactory,
                repositoriesServiceReference::get,
                searchRequestStats,
                remoteStoreStatsTrackerFactory,
                ingestionConsumerFactories,
                recoverySettings,
                cacheService,
                remoteStoreSettings,
                fileCache,
                compositeIndexSettings,
                segmentReplicator::startReplication,
                segmentReplicator::getSegmentReplicationStats
            );

            final IngestService ingestService = new IngestService(
                clusterService,
                threadPool,
                this.environment,
                scriptService,
                analysisModule.getAnalysisRegistry(),
                pluginsService.filterPlugins(IngestPlugin.class),
                client,
                indicesService,
                xContentRegistry,
                new SystemIngestPipelineCache()
            );

            final FsServiceProvider fsServiceProvider = new FsServiceProvider(
                settings,
                nodeEnvironment,
                fileCache,
                settingsModule.getClusterSettings(),
                indicesService
            );
            final MonitorService monitorService = new MonitorService(settings, threadPool, fsServiceProvider);

            final AliasValidator aliasValidator = new AliasValidator();

            final ShardLimitValidator shardLimitValidator = new ShardLimitValidator(settings, clusterService, systemIndices);
            final AwarenessReplicaBalance awarenessReplicaBalance = new AwarenessReplicaBalance(
                settings,
                clusterService.getClusterSettings()
            );
            final MetadataCreateIndexService metadataCreateIndexService = new MetadataCreateIndexService(
                settings,
                clusterService,
                indicesService,
                clusterModule.getAllocationService(),
                aliasValidator,
                shardLimitValidator,
                environment,
                settingsModule.getIndexScopedSettings(),
                threadPool,
                xContentRegistry,
                systemIndices,
                forbidPrivateIndexSettings,
                awarenessReplicaBalance,
                remoteStoreSettings,
                repositoriesServiceReference::get
            );
            pluginsService.filterPlugins(Plugin.class)
                .forEach(
                    p -> p.getAdditionalIndexSettingProviders().forEach(metadataCreateIndexService::addAdditionalIndexSettingProvider)
                );

            final MetadataCreateDataStreamService metadataCreateDataStreamService = new MetadataCreateDataStreamService(
                threadPool,
                clusterService,
                metadataCreateIndexService
            );

            final ViewService viewService = new ViewService(clusterService, client, null);

            Collection<Object> pluginComponents = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .flatMap(
                    p -> p.createComponents(
                        client,
                        clusterService,
                        threadPool,
                        resourceWatcherService,
                        scriptService,
                        xContentRegistry,
                        environment,
                        nodeEnvironment,
                        namedWriteableRegistry,
                        clusterModule.getIndexNameExpressionResolver(),
                        repositoriesServiceReference::get
                    ).stream()
                )
                .collect(Collectors.toList());

            Collection<Object> telemetryAwarePluginComponents = pluginsService.filterPlugins(TelemetryAwarePlugin.class)
                .stream()
                .flatMap(
                    p -> p.createComponents(
                        client,
                        clusterService,
                        threadPool,
                        resourceWatcherService,
                        scriptService,
                        xContentRegistry,
                        environment,
                        nodeEnvironment,
                        namedWriteableRegistry,
                        clusterModule.getIndexNameExpressionResolver(),
                        repositoriesServiceReference::get,
                        tracer,
                        metricsRegistry
                    ).stream()
                )
                .collect(Collectors.toList());

            // Add the telemetryAwarePlugin components to the existing pluginComponents collection.
            pluginComponents.addAll(telemetryAwarePluginComponents);

            List<IdentityAwarePlugin> identityAwarePlugins = pluginsService.filterPlugins(IdentityAwarePlugin.class);
            identityService.initializeIdentityAwarePlugins(identityAwarePlugins);

            final WorkloadGroupResourceUsageTrackerService workloadGroupResourceUsageTrackerService =
                new WorkloadGroupResourceUsageTrackerService(taskResourceTrackingService);
            final WorkloadManagementSettings workloadManagementSettings = new WorkloadManagementSettings(
                settings,
                settingsModule.getClusterSettings()
            );

            final WorkloadGroupsStateAccessor workloadGroupsStateAccessor = new WorkloadGroupsStateAccessor();

            final WorkloadGroupService workloadGroupService = new WorkloadGroupService(
                new WorkloadGroupTaskCancellationService(
                    workloadManagementSettings,
                    new MaximumResourceTaskSelectionStrategy(),
                    workloadGroupResourceUsageTrackerService,
                    workloadGroupsStateAccessor
                ),
                clusterService,
                threadPool,
                workloadManagementSettings,
                workloadGroupsStateAccessor
            );
            taskResourceTrackingService.addTaskCompletionListener(workloadGroupService);

            final WorkloadGroupRequestOperationListener workloadGroupRequestOperationListener = new WorkloadGroupRequestOperationListener(
                workloadGroupService,
                threadPool
            );

            // register all standard SearchRequestOperationsCompositeListenerFactory to the SearchRequestOperationsCompositeListenerFactory
            final SearchRequestOperationsCompositeListenerFactory searchRequestOperationsCompositeListenerFactory =
                new SearchRequestOperationsCompositeListenerFactory(
                    Stream.concat(
                        Stream.of(
                            searchRequestStats,
                            searchRequestSlowLog,
                            searchTaskRequestOperationsListener,
                            workloadGroupRequestOperationListener
                        ),
                        pluginComponents.stream()
                            .filter(p -> p instanceof SearchRequestOperationsListener)
                            .map(p -> (SearchRequestOperationsListener) p)
                    ).toArray(SearchRequestOperationsListener[]::new)
                );

            ActionModule actionModule = new ActionModule(
                settings,
                clusterModule.getIndexNameExpressionResolver(),
                settingsModule.getIndexScopedSettings(),
                settingsModule.getClusterSettings(),
                settingsModule.getSettingsFilter(),
                threadPool,
                pluginsService.filterPlugins(ActionPlugin.class),
                client,
                circuitBreakerService,
                usageService,
                systemIndices,
                identityService,
                extensionsManager
            );
            modules.add(actionModule);

            final RestController restController = actionModule.getRestController();

            final NodeResourceUsageTracker nodeResourceUsageTracker = new NodeResourceUsageTracker(
                monitorService.fsService(),
                threadPool,
                settings,
                clusterService.getClusterSettings()
            );
            final ResourceUsageCollectorService resourceUsageCollectorService = new ResourceUsageCollectorService(
                nodeResourceUsageTracker,
                clusterService,
                threadPool
            );

            final AdmissionControlService admissionControlService = new AdmissionControlService(
                settings,
                clusterService,
                threadPool,
                resourceUsageCollectorService
            );

            AdmissionControlTransportInterceptor admissionControlTransportInterceptor = new AdmissionControlTransportInterceptor(
                admissionControlService
            );

            WorkloadManagementTransportInterceptor workloadManagementTransportInterceptor = new WorkloadManagementTransportInterceptor(
                threadPool,
                workloadGroupService
            );

            this.autoForceMergeManager = new AutoForceMergeManager(threadPool, monitorService, indicesService, clusterService);

            final Collection<SecureSettingsFactory> secureSettingsFactories = pluginsService.filterPlugins(Plugin.class)
                .stream()
                .map(p -> p.getSecureSettingFactory(settings))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

            List<TransportInterceptor> transportInterceptors = List.of(
                admissionControlTransportInterceptor,
                workloadManagementTransportInterceptor
            );
            final NetworkModule networkModule = new NetworkModule(
                settings,
                pluginsService.filterPlugins(NetworkPlugin.class),
                threadPool,
                bigArrays,
                pageCacheRecycler,
                circuitBreakerService,
                namedWriteableRegistry,
                xContentRegistry,
                networkService,
                restController,
                clusterService.getClusterSettings(),
                tracer,
                transportInterceptors,
                secureSettingsFactories
            );

            Collection<UnaryOperator<Map<String, IndexTemplateMetadata>>> indexTemplateMetadataUpgraders = pluginsService.filterPlugins(
                Plugin.class
            ).stream().map(Plugin::getIndexTemplateMetadataUpgrader).collect(Collectors.toList());
            final MetadataUpgrader metadataUpgrader = new MetadataUpgrader(indexTemplateMetadataUpgraders);
            final MetadataIndexUpgradeService metadataIndexUpgradeService = new MetadataIndexUpgradeService(
                settings,
                xContentRegistry,
                indicesModule.getMapperRegistry(),
                settingsModule.getIndexScopedSettings(),
                systemIndices,
                scriptService
            );
            if (DiscoveryNode.isClusterManagerNode(settings)) {
                clusterService.addListener(new SystemIndexMetadataUpgradeService(systemIndices, clusterService));
            }
            new TemplateUpgradeService(client, clusterService, threadPool, indexTemplateMetadataUpgraders);
            final Transport transport = networkModule.getTransportSupplier().get();
            final Supplier<Transport> streamTransportSupplier = networkModule.getStreamTransportSupplier();
            if (FeatureFlags.isEnabled(STREAM_TRANSPORT) && streamTransportSupplier == null) {
                throw new IllegalStateException(STREAM_TRANSPORT + " is enabled but no stream transport supplier is provided");
            }
            final Transport streamTransport = (streamTransportSupplier != null ? streamTransportSupplier.get() : null);

            Set<String> taskHeaders = Stream.concat(
                pluginsService.filterPlugins(ActionPlugin.class).stream().flatMap(p -> p.getTaskHeaders().stream()),
                Stream.of(Task.X_OPAQUE_ID)
            ).collect(Collectors.toSet());

            final TransportService transportService = newTransportService(
                settings,
                transport,
                streamTransport,
                threadPool,
                networkModule.getTransportInterceptor(),
                localNodeFactory,
                settingsModule.getClusterSettings(),
                taskHeaders,
                tracer
            );
            final Optional<StreamTransportService> streamTransportService = streamTransport != null
                ? Optional.of(
                    new StreamTransportService(
                        settings,
                        streamTransport,
                        threadPool,
                        networkModule.getTransportInterceptor(),
                        new LocalNodeFactory(settings, nodeEnvironment.nodeId(), remoteStoreNodeService),
                        settingsModule.getClusterSettings(),
                        transportService.getTaskManager(),
                        transportService.getRemoteClusterService(),
                        tracer
                    )
                )
                : Optional.empty();

            TopNSearchTasksLogger taskConsumer = new TopNSearchTasksLogger(settings, settingsModule.getClusterSettings());
            transportService.getTaskManager().registerTaskResourceConsumer(taskConsumer);
            streamTransportService.ifPresent(service -> service.getTaskManager().registerTaskResourceConsumer(taskConsumer));
            this.extensionsManager.initializeServicesAndRestHandler(
                actionModule,
                settingsModule,
                transportService,
                clusterService,
                environment.settings(),
                client,
                identityService
            );
            final PersistedStateRegistry persistedStateRegistry = new PersistedStateRegistry();
            final GatewayMetaState gatewayMetaState = new GatewayMetaState();
            final ResponseCollectorService responseCollectorService = new ResponseCollectorService(clusterService);
            final SearchTransportService searchTransportService = new SearchTransportService(
                transportService,
                SearchExecutionStatsCollector.makeWrapper(responseCollectorService)
            );
            final Optional<StreamSearchTransportService> streamSearchTransportService = streamTransportService.map(
                stc -> new StreamSearchTransportService(stc, SearchExecutionStatsCollector.makeWrapper(responseCollectorService))
            );
            final HttpServerTransport httpServerTransport = newHttpTransport(networkModule);

            pluginComponents.addAll(newAuxTransports(networkModule));

            final IndexingPressureService indexingPressureService = new IndexingPressureService(settings, clusterService);
            // Going forward, IndexingPressureService will have required constructs for exposing listeners/interfaces for plugin
            // development. Then we can deprecate Getter and Setter for IndexingPressureService in ClusterService (#478).
            clusterService.setIndexingPressureService(indexingPressureService);

            final SearchBackpressureSettings searchBackpressureSettings = new SearchBackpressureSettings(
                settings,
                clusterService.getClusterSettings()
            );

            final SearchBackpressureService searchBackpressureService = new SearchBackpressureService(
                searchBackpressureSettings,
                taskResourceTrackingService,
                threadPool,
                transportService.getTaskManager(),
                workloadGroupService
            );

            final SegmentReplicationStatsTracker segmentReplicationStatsTracker = new SegmentReplicationStatsTracker(indicesService);
            RepositoriesModule repositoriesModule = new RepositoriesModule(
                this.environment,
                pluginsService.filterPlugins(RepositoryPlugin.class),
                transportService,
                clusterService,
                threadPool,
                xContentRegistry,
                recoverySettings
            );
            CryptoHandlerRegistry.initRegistry(
                pluginsService.filterPlugins(CryptoPlugin.class),
                pluginsService.filterPlugins(CryptoKeyProviderPlugin.class),
                settings
            );
            RepositoriesService repositoryService = repositoriesModule.getRepositoryService();
            repositoriesServiceReference.set(repositoryService);
            SnapshotsService snapshotsService = new SnapshotsService(
                settings,
                clusterService,
                clusterModule.getIndexNameExpressionResolver(),
                repositoryService,
                transportService,
                actionModule.getActionFilters(),
                remoteStorePinnedTimestampService,
                remoteStoreSettings
            );
            SnapshotShardsService snapshotShardsService = new SnapshotShardsService(
                settings,
                clusterService,
                repositoryService,
                transportService,
                indicesService
            );
            TransportNodesSnapshotsStatus nodesSnapshotsStatus = new TransportNodesSnapshotsStatus(
                threadPool,
                clusterService,
                transportService,
                snapshotShardsService,
                actionModule.getActionFilters()
            );
            RestoreService restoreService = new RestoreService(
                clusterService,
                repositoryService,
                clusterModule.getAllocationService(),
                metadataCreateIndexService,
                metadataIndexUpgradeService,
                shardLimitValidator,
                indicesService,
                clusterInfoService::getClusterInfo,
                new FileCacheSettings(settings, clusterService.getClusterSettings())::getRemoteDataRatio
            );

            RemoteStoreRestoreService remoteStoreRestoreService = new RemoteStoreRestoreService(
                clusterService,
                clusterModule.getAllocationService(),
                metadataCreateIndexService,
                metadataIndexUpgradeService,
                shardLimitValidator,
                remoteClusterStateService
            );

            final DiskThresholdMonitor diskThresholdMonitor = new DiskThresholdMonitor(
                settings,
                clusterService::state,
                clusterService.getClusterSettings(),
                client,
                threadPool::relativeTimeInMillis,
                rerouteService,
                new FileCacheSettings(settings, clusterService.getClusterSettings())::getRemoteDataRatio
            );
            clusterInfoService.addListener(diskThresholdMonitor::onNewInfo);

            final Discovery discovery;
            if (clusterless) {
                discovery = new LocalDiscovery(transportService, clusterService.getClusterApplierService());
            } else {
                discovery = new DiscoveryModule(
                    settings,
                    threadPool,
                    transportService,
                    namedWriteableRegistry,
                    networkService,
                    clusterService.getClusterManagerService(),
                    clusterService.getClusterApplierService(),
                    clusterService.getClusterSettings(),
                    pluginsService.filterPlugins(DiscoveryPlugin.class),
                    clusterModule.getAllocationService(),
                    environment.configDir(),
                    gatewayMetaState,
                    rerouteService,
                    fsHealthService,
                    persistedStateRegistry,
                    remoteStoreNodeService,
                    clusterManagerMetrics,
                    remoteClusterStateService
                ).getDiscovery();
            }
            final SearchPipelineService searchPipelineService = new SearchPipelineService(
                clusterService,
                threadPool,
                this.environment,
                scriptService,
                analysisModule.getAnalysisRegistry(),
                xContentRegistry,
                namedWriteableRegistry,
                pluginsService.filterPlugins(SearchPipelinePlugin.class),
                client
            );
            final TaskCancellationMonitoringSettings taskCancellationMonitoringSettings = new TaskCancellationMonitoringSettings(
                settings,
                clusterService.getClusterSettings()
            );
            final TaskCancellationMonitoringService taskCancellationMonitoringService = new TaskCancellationMonitoringService(
                threadPool,
                transportService.getTaskManager(),
                taskCancellationMonitoringSettings
            );

            this.nodeService = new NodeService(
                settings,
                threadPool,
                monitorService,
                discovery,
                transportService,
                indicesService,
                pluginsService,
                circuitBreakerService,
                scriptService,
                httpServerTransport,
                ingestService,
                clusterService,
                settingsModule.getSettingsFilter(),
                responseCollectorService,
                searchTransportService,
                indexingPressureService,
                searchModule.getValuesSourceRegistry().getUsageService(),
                searchBackpressureService,
                searchPipelineService,
                fileCache,
                taskCancellationMonitoringService,
                resourceUsageCollectorService,
                segmentReplicationStatsTracker,
                repositoryService,
                admissionControlService,
                cacheService
            );

            if (FeatureFlags.isEnabled(ARROW_STREAMS_SETTING)) {
                final List<StreamManagerPlugin> streamManagerPlugins = pluginsService.filterPlugins(StreamManagerPlugin.class);

                final List<StreamManager> streamManagers = streamManagerPlugins.stream()
                    .map(StreamManagerPlugin::getStreamManager)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .toList();

                if (streamManagers.size() > 1) {
                    throw new IllegalStateException(
                        String.format(Locale.ROOT, "Only one StreamManagerPlugin can be installed. Found: %d", streamManagerPlugins.size())
                    );
                } else if (streamManagers.isEmpty() == false) {
                    StreamManager streamManager = streamManagers.getFirst();
                    streamManagerPlugins.forEach(plugin -> plugin.onStreamManagerInitialized(streamManager));
                }
            }

            final SearchService searchService = newSearchService(
                clusterService,
                indicesService,
                threadPool,
                scriptService,
                bigArrays,
                searchModule.getQueryPhase(),
                searchModule.getFetchPhase(),
                responseCollectorService,
                circuitBreakerService,
                searchModule.getIndexSearcherExecutor(threadPool),
                taskResourceTrackingService,
                searchModule.getConcurrentSearchRequestDeciderFactories(),
                searchModule.getPluginProfileMetricsProviders()
            );

            final List<PersistentTasksExecutor<?>> tasksExecutors = pluginsService.filterPlugins(PersistentTaskPlugin.class)
                .stream()
                .map(
                    p -> p.getPersistentTasksExecutor(
                        clusterService,
                        threadPool,
                        client,
                        settingsModule,
                        clusterModule.getIndexNameExpressionResolver()
                    )
                )
                .flatMap(List::stream)
                .collect(toList());

            final Optional<TaskManagerClient> taskManagerClientOptional = FeatureFlags.isEnabled(BACKGROUND_TASK_EXECUTION_EXPERIMENTAL)
                ? pluginsService.filterPlugins(TaskManagerClientPlugin.class)
                    .stream()
                    .map(plugin -> plugin.getTaskManagerClient(client, clusterService, threadPool))
                    .findFirst()
                : Optional.empty();

            final PersistentTasksExecutorRegistry registry = new PersistentTasksExecutorRegistry(tasksExecutors);
            final PersistentTasksClusterService persistentTasksClusterService = new PersistentTasksClusterService(
                settings,
                registry,
                clusterService,
                threadPool
            );
            resourcesToClose.add(persistentTasksClusterService);
            final PersistentTasksService persistentTasksService = new PersistentTasksService(clusterService, threadPool, client);

            mergedSegmentWarmerFactory = new MergedSegmentWarmerFactory(transportService, recoverySettings, clusterService);

            final MappingTransformerRegistry mappingTransformerRegistry = new MappingTransformerRegistry(mapperPlugins, xContentRegistry);

            modules.add(b -> {
                b.bind(Node.class).toInstance(this);
                b.bind(NodeService.class).toInstance(nodeService);
                b.bind(NamedXContentRegistry.class).toInstance(xContentRegistry);
                b.bind(PluginsService.class).toInstance(pluginsService);
                b.bind(Client.class).toInstance(client);
                b.bind(NodeClient.class).toInstance(client);
                b.bind(Environment.class).toInstance(this.environment);
                b.bind(ExtensionsManager.class).toInstance(this.extensionsManager);
                b.bind(ThreadPool.class).toInstance(threadPool);
                b.bind(NodeEnvironment.class).toInstance(nodeEnvironment);
                b.bind(ResourceWatcherService.class).toInstance(resourceWatcherService);
                b.bind(CircuitBreakerService.class).toInstance(circuitBreakerService);
                b.bind(BigArrays.class).toInstance(bigArrays);
                b.bind(PageCacheRecycler.class).toInstance(pageCacheRecycler);
                b.bind(ScriptService.class).toInstance(scriptService);
                b.bind(AnalysisRegistry.class).toInstance(analysisModule.getAnalysisRegistry());
                b.bind(IngestService.class).toInstance(ingestService);
                b.bind(SearchPipelineService.class).toInstance(searchPipelineService);
                b.bind(IndexingPressureService.class).toInstance(indexingPressureService);
                b.bind(TaskResourceTrackingService.class).toInstance(taskResourceTrackingService);
                b.bind(SearchBackpressureService.class).toInstance(searchBackpressureService);
                b.bind(WorkloadGroupService.class).toInstance(workloadGroupService);
                b.bind(AdmissionControlService.class).toInstance(admissionControlService);
                b.bind(UsageService.class).toInstance(usageService);
                b.bind(AggregationUsageService.class).toInstance(searchModule.getValuesSourceRegistry().getUsageService());
                b.bind(NamedWriteableRegistry.class).toInstance(namedWriteableRegistry);
                b.bind(MetadataUpgrader.class).toInstance(metadataUpgrader);
                b.bind(MetaStateService.class).toInstance(metaStateService);
                b.bind(PersistedClusterStateService.class).toInstance(lucenePersistedStateFactory);
                b.bind(IndicesService.class).toInstance(indicesService);
                b.bind(RemoteStoreStatsTrackerFactory.class).toInstance(remoteStoreStatsTrackerFactory);
                b.bind(AliasValidator.class).toInstance(aliasValidator);
                b.bind(MetadataCreateIndexService.class).toInstance(metadataCreateIndexService);
                b.bind(AwarenessReplicaBalance.class).toInstance(awarenessReplicaBalance);
                b.bind(MetadataCreateDataStreamService.class).toInstance(metadataCreateDataStreamService);
                b.bind(ViewService.class).toInstance(viewService);
                b.bind(SearchService.class).toInstance(searchService);
                b.bind(SearchTransportService.class).toInstance(searchTransportService);
                if (streamSearchTransportService.isPresent()) {
                    b.bind(StreamSearchTransportService.class).toInstance(streamSearchTransportService.get());
                } else {
                    b.bind(StreamSearchTransportService.class).toProvider((Providers.of(null)));
                }
                b.bind(SearchPhaseController.class)
                    .toInstance(new SearchPhaseController(namedWriteableRegistry, searchService::aggReduceContextBuilder));
                b.bind(Transport.class).toInstance(transport);
                b.bind(TransportService.class).toInstance(transportService);
                if (streamTransportService.isPresent()) {
                    b.bind(StreamTransportService.class).toInstance(streamTransportService.get());
                } else {
                    b.bind(StreamTransportService.class).toProvider((Providers.of(null)));
                }
                b.bind(NetworkService.class).toInstance(networkService);
                b.bind(UpdateHelper.class).toInstance(new UpdateHelper(scriptService));
                b.bind(MetadataIndexUpgradeService.class).toInstance(metadataIndexUpgradeService);
                b.bind(ClusterInfoService.class).toInstance(clusterInfoService);
                b.bind(SnapshotsInfoService.class).toInstance(snapshotsInfoService);
                b.bind(GatewayMetaState.class).toInstance(gatewayMetaState);
                b.bind(Discovery.class).toInstance(discovery);
                b.bind(RemoteStoreSettings.class).toInstance(remoteStoreSettings);
                {
                    b.bind(PeerRecoverySourceService.class)
                        .toInstance(new PeerRecoverySourceService(transportService, indicesService, recoverySettings));
                    b.bind(PeerRecoveryTargetService.class)
                        .toInstance(new PeerRecoveryTargetService(threadPool, transportService, recoverySettings, clusterService));
                    b.bind(SegmentReplicationTargetService.class)
                        .toInstance(
                            new SegmentReplicationTargetService(
                                threadPool,
                                recoverySettings,
                                transportService,
                                new SegmentReplicationSourceFactory(transportService, recoverySettings, clusterService),
                                indicesService,
                                clusterService,
                                segmentReplicator
                            )
                        );
                    b.bind(SegmentReplicationSourceService.class)
                        .toInstance(new SegmentReplicationSourceService(indicesService, transportService, recoverySettings));
                }
                b.bind(HttpServerTransport.class).toInstance(httpServerTransport);
                pluginComponents.stream().forEach(p -> b.bind((Class) p.getClass()).toInstance(p));
                b.bind(PersistentTasksService.class).toInstance(persistentTasksService);
                b.bind(PersistentTasksClusterService.class).toInstance(persistentTasksClusterService);
                b.bind(PersistentTasksExecutorRegistry.class).toInstance(registry);
                b.bind(RepositoriesService.class).toInstance(repositoryService);
                b.bind(SnapshotsService.class).toInstance(snapshotsService);
                b.bind(SnapshotShardsService.class).toInstance(snapshotShardsService);
                b.bind(TransportNodesSnapshotsStatus.class).toInstance(nodesSnapshotsStatus);
                b.bind(RestoreService.class).toInstance(restoreService);
                b.bind(RemoteStoreRestoreService.class).toInstance(remoteStoreRestoreService);
                b.bind(RerouteService.class).toInstance(rerouteService);
                b.bind(ShardLimitValidator.class).toInstance(shardLimitValidator);
                b.bind(FsHealthService.class).toInstance(fsHealthService);
                b.bind(NodeResourceUsageTracker.class).toInstance(nodeResourceUsageTracker);
                b.bind(ResourceUsageCollectorService.class).toInstance(resourceUsageCollectorService);
                b.bind(SystemIndices.class).toInstance(systemIndices);
                b.bind(IdentityService.class).toInstance(identityService);
                b.bind(Tracer.class).toInstance(tracer);
                b.bind(SearchRequestStats.class).toInstance(searchRequestStats);
                b.bind(SearchRequestSlowLog.class).toInstance(searchRequestSlowLog);
                b.bind(MetricsRegistry.class).toInstance(metricsRegistry);
                b.bind(RemoteClusterStateService.class).toProvider(() -> remoteClusterStateService);
                b.bind(RemoteIndexPathUploader.class).toProvider(() -> remoteIndexPathUploader);
                b.bind(RemoteStorePinnedTimestampService.class).toProvider(() -> remoteStorePinnedTimestampService);
                b.bind(RemoteClusterStateCleanupManager.class).toProvider(() -> remoteClusterStateCleanupManager);
                b.bind(PersistedStateRegistry.class).toInstance(persistedStateRegistry);
                b.bind(SegmentReplicationStatsTracker.class).toInstance(segmentReplicationStatsTracker);
                b.bind(SearchRequestOperationsCompositeListenerFactory.class).toInstance(searchRequestOperationsCompositeListenerFactory);
                b.bind(SegmentReplicator.class).toInstance(segmentReplicator);
                b.bind(MergedSegmentWarmerFactory.class).toInstance(mergedSegmentWarmerFactory);
                b.bind(MappingTransformerRegistry.class).toInstance(mappingTransformerRegistry);
                b.bind(AutoForceMergeManager.class).toInstance(autoForceMergeManager);
                if (FeatureFlags.isEnabled(FeatureFlags.MERGED_SEGMENT_WARMER_EXPERIMENTAL_FLAG)) {
                    if (isRemoteDataAttributePresent(settings)) {
                        b.bind(MergedSegmentPublisher.PublishAction.class)
                            .to(RemoteStorePublishMergedSegmentAction.class)
                            .asEagerSingleton();
                    } else {
                        b.bind(MergedSegmentPublisher.PublishAction.class).to(PublishMergedSegmentAction.class).asEagerSingleton();
                    }
                } else {
                    b.bind(MergedSegmentPublisher.PublishAction.class).toInstance((shard, checkpoint) -> {});
                }
                b.bind(MergedSegmentPublisher.class).asEagerSingleton();

                taskManagerClientOptional.ifPresent(value -> b.bind(TaskManagerClient.class).toInstance(value));
            });
            injector = modules.createInjector();

            // We allocate copies of existing shards by looking for a viable copy of the shard in the cluster and assigning the shard there.
            // The search for viable copies is triggered by an allocation attempt (i.e. a reroute) and is performed asynchronously. When it
            // completes we trigger another reroute to try the allocation again. This means there is a circular dependency: the allocation
            // service needs access to the existing shards allocators (e.g. the GatewayAllocator, ShardsBatchGatewayAllocator) which
            // need to be able to trigger a reroute, which needs to call into the allocation service. We close the loop here:
            clusterModule.setExistingShardsAllocators(
                injector.getInstance(GatewayAllocator.class),
                injector.getInstance(ShardsBatchGatewayAllocator.class)
            );

            List<LifecycleComponent> pluginLifecycleComponents = pluginComponents.stream()
                .filter(p -> p instanceof LifecycleComponent)
                .map(p -> (LifecycleComponent) p)
                .collect(Collectors.toList());
            pluginLifecycleComponents.addAll(
                pluginsService.getGuiceServiceClasses().stream().map(injector::getInstance).collect(Collectors.toList())
            );
            resourcesToClose.addAll(pluginLifecycleComponents);
            resourcesToClose.add(injector.getInstance(PeerRecoverySourceService.class));
            this.pluginLifecycleComponents = Collections.unmodifiableList(pluginLifecycleComponents);
            DynamicActionRegistry dynamicActionRegistry = actionModule.getDynamicActionRegistry();
            dynamicActionRegistry.registerUnmodifiableActionMap(injector.getInstance(new Key<Map<ActionType, TransportAction>>() {
            }));
            client.initialize(
                dynamicActionRegistry,
                () -> clusterService.localNode().getId(),
                transportService.getRemoteClusterService(),
                namedWriteableRegistry
            );
            this.namedWriteableRegistry = namedWriteableRegistry;

            logger.debug("initializing HTTP handlers ...");
            actionModule.initRestHandlers(() -> clusterService.state().nodes());
            logger.info("initialized");

            success = true;
        } catch (IOException ex) {
            throw new DensityException("failed to bind service", ex);
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(resourcesToClose);
            }
        }
    }

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
        return new TransportService(
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

    /**
     * The settings that are used by this node. Contains original settings as well as additional settings provided by plugins.
     */
    public Settings settings() {
        return this.environment.settings();
    }

    /**
     * A client that can be used to execute actions (operations) against the cluster.
     */
    public Client client() {
        return client;
    }

    /**
     * Returns the environment of the node
     */
    public Environment getEnvironment() {
        return environment;
    }

    /**
     * Returns the {@link NodeEnvironment} instance of this node
     */
    public NodeEnvironment getNodeEnvironment() {
        return nodeEnvironment;
    }

    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    public Node start() throws NodeValidationException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }

        logger.info("starting ...");
        pluginLifecycleComponents.forEach(LifecycleComponent::start);

        injector.getInstance(MappingUpdatedAction.class).setClient(client);
        injector.getInstance(IndicesService.class).start();
        injector.getInstance(IndicesClusterStateService.class).start();
        injector.getInstance(SnapshotsService.class).start();
        injector.getInstance(SnapshotShardsService.class).start();
        injector.getInstance(RepositoriesService.class).start();
        injector.getInstance(SearchService.class).start();
        injector.getInstance(FsHealthService.class).start();
        injector.getInstance(NodeResourceUsageTracker.class).start();
        injector.getInstance(ResourceUsageCollectorService.class).start();
        nodeService.getMonitorService().start();
        nodeService.getSearchBackpressureService().start();
        nodeService.getTaskCancellationMonitoringService().start();
        injector.getInstance(WorkloadGroupService.class).start();

        final ClusterService clusterService = injector.getInstance(ClusterService.class);

        final NodeConnectionsService nodeConnectionsService = injector.getInstance(NodeConnectionsService.class);
        nodeConnectionsService.start();
        clusterService.setNodeConnectionsService(nodeConnectionsService);
        StreamTransportService streamTransportService = injector.getInstance(StreamTransportService.class);
        if (streamTransportService != null) {
            final StreamNodeConnectionsService streamNodeConnectionsService = injector.getInstance(StreamNodeConnectionsService.class);
            streamNodeConnectionsService.start();
            clusterService.setStreamNodeConnectionsService(streamNodeConnectionsService);
        }

        injector.getInstance(GatewayService.class).start();
        Discovery discovery = injector.getInstance(Discovery.class);
        discovery.setNodeConnectionsService(nodeConnectionsService);
        clusterService.getClusterManagerService().setClusterStatePublisher(discovery);

        // Start the transport service now so the publish address will be added to the local disco node in ClusterService
        TransportService transportService = injector.getInstance(TransportService.class);
        transportService.getTaskManager().setTaskResultsService(injector.getInstance(TaskResultsService.class));
        transportService.getTaskManager().setTaskCancellationService(new TaskCancellationService(transportService));

        TaskResourceTrackingService taskResourceTrackingService = injector.getInstance(TaskResourceTrackingService.class);
        transportService.getTaskManager().setTaskResourceTrackingService(taskResourceTrackingService);

        runnableTaskListener.set(taskResourceTrackingService);
        // start streamTransportService before transportService so that transport service has access to publish address
        // of stream transport for it to use it in localNode creation
        if (streamTransportService != null) {
            streamTransportService.start();
        }
        transportService.start();
        assert localNodeFactory.getNode() != null;
        assert transportService.getLocalNode().equals(localNodeFactory.getNode())
            : "transportService has a different local node than the factory provided";
        injector.getInstance(PeerRecoverySourceService.class).start();
        injector.getInstance(SegmentReplicationTargetService.class).start();
        injector.getInstance(SegmentReplicationSourceService.class).start();

        final RemoteClusterStateService remoteClusterStateService = injector.getInstance(RemoteClusterStateService.class);
        if (remoteClusterStateService != null) {
            remoteClusterStateService.start();
        }
        final RemoteIndexPathUploader remoteIndexPathUploader = injector.getInstance(RemoteIndexPathUploader.class);
        if (remoteIndexPathUploader != null) {
            remoteIndexPathUploader.start();
        }
        final RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = injector.getInstance(
            RemoteStorePinnedTimestampService.class
        );
        if (remoteStorePinnedTimestampService != null) {
            remoteStorePinnedTimestampService.start();
        }
        // Load (and maybe upgrade) the metadata stored on disk
        final GatewayMetaState gatewayMetaState = injector.getInstance(GatewayMetaState.class);
        gatewayMetaState.start(
            settings(),
            transportService,
            clusterService,
            injector.getInstance(MetaStateService.class),
            injector.getInstance(MetadataIndexUpgradeService.class),
            injector.getInstance(MetadataUpgrader.class),
            injector.getInstance(PersistedClusterStateService.class),
            injector.getInstance(RemoteClusterStateService.class),
            injector.getInstance(PersistedStateRegistry.class),
            injector.getInstance(RemoteStoreRestoreService.class)
        );
        if (Assertions.ENABLED) {
            try {
                assert injector.getInstance(MetaStateService.class).loadFullState().v1().isEmpty();
                final NodeMetadata nodeMetadata = NodeMetadata.FORMAT.loadLatestState(
                    logger,
                    NamedXContentRegistry.EMPTY,
                    nodeEnvironment.nodeDataPaths()
                );
                assert nodeMetadata != null;
                assert nodeMetadata.nodeVersion().equals(Version.CURRENT);
                assert nodeMetadata.nodeId().equals(localNodeFactory.getNode().getId());
            } catch (IOException e) {
                assert false : e;
            }
        }
        // we load the global state here (the persistent part of the cluster state stored on disk) to
        // pass it to the bootstrap checks to allow plugins to enforce certain preconditions based on the recovered state.
        final Metadata onDiskMetadata = gatewayMetaState.getPersistedState().getLastAcceptedState().metadata();
        assert onDiskMetadata != null : "metadata is null but shouldn't"; // this is never null
        validateNodeBeforeAcceptingRequests(
            new BootstrapContext(environment, onDiskMetadata),
            transportService.boundAddress(),
            pluginsService.filterPlugins(Plugin.class).stream().flatMap(p -> p.getBootstrapChecks().stream()).collect(Collectors.toList())
        );

        clusterService.addStateApplier(transportService.getTaskManager());
        // start after transport service so the local disco is known
        discovery.start(); // start before cluster service so that it can set initial state on ClusterApplierService
        clusterService.start();
        this.autoForceMergeManager.start();
        assert clusterService.localNode().equals(localNodeFactory.getNode())
            : "clusterService has a different local node than the factory provided";
        transportService.acceptIncomingRequests();
        if (streamTransportService != null) {
            streamTransportService.acceptIncomingRequests();
        }

        discovery.startInitialJoin();
        final TimeValue initialStateTimeout = DiscoverySettings.INITIAL_STATE_TIMEOUT_SETTING.get(settings());
        configureNodeAndClusterIdStateListener(clusterService);

        if (initialStateTimeout.millis() > 0) {
            final ThreadPool thread = injector.getInstance(ThreadPool.class);
            ClusterState clusterState = clusterService.state();
            ClusterStateObserver observer = new ClusterStateObserver(clusterState, clusterService, null, logger, thread.getThreadContext());

            if (clusterState.nodes().getClusterManagerNodeId() == null) {
                logger.debug("waiting to join the cluster. timeout [{}]", initialStateTimeout);
                final CountDownLatch latch = new CountDownLatch(1);
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        latch.countDown();
                    }

                    @Override
                    public void onClusterServiceClose() {
                        latch.countDown();
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        logger.warn("timed out while waiting for initial discovery state - timeout: {}", initialStateTimeout);
                        latch.countDown();
                    }
                }, state -> state.nodes().getClusterManagerNodeId() != null, initialStateTimeout);

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new DensityTimeoutException("Interrupted while waiting for initial discovery state");
                }
            }
        }

        injector.getInstance(HttpServerTransport.class).start();

        if (WRITE_PORTS_FILE_SETTING.get(settings())) {
            TransportService transport = injector.getInstance(TransportService.class);
            writePortsFile("transport", transport.boundAddress());
            HttpServerTransport http = injector.getInstance(HttpServerTransport.class);
            writePortsFile("http", http.boundAddress());
        }

        logger.info("started");

        pluginsService.filterPlugins(ClusterPlugin.class).forEach(plugin -> plugin.onNodeStarted(clusterService.localNode()));

        return this;
    }

    protected void configureNodeAndClusterIdStateListener(ClusterService clusterService) {
        NodeAndClusterIdStateListener.getAndSetNodeIdAndClusterId(
            clusterService,
            injector.getInstance(ThreadPool.class).getThreadContext()
        );
    }

    private Node stop() {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        logger.info("stopping ...");

        injector.getInstance(ResourceWatcherService.class).close();
        injector.getInstance(HttpServerTransport.class).stop();

        injector.getInstance(SnapshotsService.class).stop();
        injector.getInstance(SnapshotShardsService.class).stop();
        injector.getInstance(RepositoriesService.class).stop();
        // stop any changes happening as a result of cluster state changes
        injector.getInstance(IndicesClusterStateService.class).stop();
        // close discovery early to not react to pings anymore.
        // This can confuse other nodes and delay things - mostly if we're the cluster manager and we're running tests.
        injector.getInstance(Discovery.class).stop();
        // we close indices first, so operations won't be allowed on it
        injector.getInstance(ClusterService.class).stop();
        injector.getInstance(NodeConnectionsService.class).stop();
        injector.getInstance(FsHealthService.class).stop();
        injector.getInstance(NodeResourceUsageTracker.class).stop();
        injector.getInstance(ResourceUsageCollectorService.class).stop();
        injector.getInstance(WorkloadGroupService.class).stop();
        nodeService.getMonitorService().stop();
        nodeService.getSearchBackpressureService().stop();
        injector.getInstance(GatewayService.class).stop();
        injector.getInstance(SearchService.class).stop();
        injector.getInstance(TransportService.class).stop();
        StreamTransportService stc = injector.getInstance(StreamTransportService.class);
        if (stc != null) {
            stc.stop();
        }
        nodeService.getTaskCancellationMonitoringService().stop();
        autoForceMergeManager.stop();
        pluginLifecycleComponents.forEach(LifecycleComponent::stop);
        // we should stop this last since it waits for resources to get released
        // if we had scroll searchers etc or recovery going on we wait for to finish.
        injector.getInstance(IndicesService.class).stop();
        logger.info("stopped");

        return this;
    }

    // During concurrent close() calls we want to make sure that all of them return after the node has completed it's shutdown cycle.
    // If not, the hook that is added in Bootstrap#setup() will be useless:
    // close() might not be executed, in case another (for example api) call to close() has already set some lifecycles to stopped.
    // In this case the process will be terminated even if the first call to close() has not finished yet.
    @Override
    public synchronized void close() throws IOException {
        synchronized (lifecycle) {
            if (lifecycle.started()) {
                stop();
            }
            if (!lifecycle.moveToClosed()) {
                return;
            }
        }

        logger.info("closing ...");
        List<Closeable> toClose = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("node_close");
        toClose.add(() -> stopWatch.start("node_service"));
        toClose.add(nodeService);
        toClose.add(() -> stopWatch.stop().start("http"));
        toClose.add(injector.getInstance(HttpServerTransport.class));
        toClose.add(() -> stopWatch.stop().start("snapshot_service"));
        toClose.add(injector.getInstance(SnapshotsService.class));
        toClose.add(injector.getInstance(SnapshotShardsService.class));
        toClose.add(injector.getInstance(RepositoriesService.class));
        toClose.add(() -> stopWatch.stop().start("client"));
        Releasables.close(injector.getInstance(Client.class));
        toClose.add(() -> stopWatch.stop().start("indices_cluster"));
        toClose.add(injector.getInstance(IndicesClusterStateService.class));
        toClose.add(() -> stopWatch.stop().start("indices"));
        toClose.add(injector.getInstance(IndicesService.class));
        // close filter/fielddata caches after indices
        toClose.add(injector.getInstance(IndicesStore.class));
        toClose.add(injector.getInstance(PeerRecoverySourceService.class));
        toClose.add(injector.getInstance(SegmentReplicationSourceService.class));
        toClose.add(injector.getInstance(SegmentReplicationTargetService.class));
        toClose.add(() -> stopWatch.stop().start("cluster"));
        toClose.add(injector.getInstance(ClusterService.class));
        toClose.add(() -> stopWatch.stop().start("node_connections_service"));
        toClose.add(injector.getInstance(NodeConnectionsService.class));
        toClose.add(() -> stopWatch.stop().start("discovery"));
        toClose.add(injector.getInstance(Discovery.class));
        toClose.add(() -> stopWatch.stop().start("monitor"));
        toClose.add(nodeService.getMonitorService());
        toClose.add(nodeService.getSearchBackpressureService());
        toClose.add(() -> stopWatch.stop().start("fsHealth"));
        toClose.add(injector.getInstance(FsHealthService.class));
        toClose.add(() -> stopWatch.stop().start("resource_usage_tracker"));
        toClose.add(injector.getInstance(NodeResourceUsageTracker.class));
        toClose.add(() -> stopWatch.stop().start("resource_usage_collector"));
        toClose.add(injector.getInstance(ResourceUsageCollectorService.class));
        toClose.add(() -> stopWatch.stop().start("gateway"));
        toClose.add(injector.getInstance(GatewayService.class));
        toClose.add(() -> stopWatch.stop().start("search"));
        toClose.add(injector.getInstance(SearchService.class));
        toClose.add(() -> stopWatch.stop().start("transport"));
        toClose.add(injector.getInstance(TransportService.class));
        StreamTransportService stc = injector.getInstance(StreamTransportService.class);
        if (stc != null) {
            toClose.add(stc);
        }
        toClose.add(nodeService.getTaskCancellationMonitoringService());
        toClose.add(injector.getInstance(RemoteStorePinnedTimestampService.class));

        for (LifecycleComponent plugin : pluginLifecycleComponents) {
            toClose.add(() -> stopWatch.stop().start("plugin(" + plugin.getClass().getName() + ")"));
            toClose.add(plugin);
        }
        toClose.addAll(pluginsService.filterPlugins(Plugin.class));

        toClose.add(() -> stopWatch.stop().start("script"));
        toClose.add(injector.getInstance(ScriptService.class));

        toClose.add(() -> stopWatch.stop().start("thread_pool"));
        toClose.add(() -> injector.getInstance(ThreadPool.class).shutdown());
        // Don't call shutdownNow here, it might break ongoing operations on Lucene indices.
        // See https://issues.apache.org/jira/browse/LUCENE-7248. We call shutdownNow in
        // awaitClose if the node doesn't finish closing within the specified time.

        toClose.add(() -> stopWatch.stop().start("gateway_meta_state"));
        toClose.add(injector.getInstance(GatewayMetaState.class));

        toClose.add(() -> stopWatch.stop().start("node_environment"));
        toClose.add(injector.getInstance(NodeEnvironment.class));
        toClose.add(stopWatch::stop);
        if (FeatureFlags.isEnabled(TELEMETRY)) {
            toClose.add(injector.getInstance(Tracer.class));
            toClose.add(injector.getInstance(MetricsRegistry.class));
        }

        if (logger.isTraceEnabled()) {
            toClose.add(() -> logger.trace("Close times for each service:\n{}", stopWatch.prettyPrint()));
        }
        autoForceMergeManager.stop();
        IOUtils.close(toClose);
        logger.info("closed");
    }

    /**
     * Wait for this node to be effectively closed.
     */
    // synchronized to prevent running concurrently with close()
    public synchronized boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        if (lifecycle.closed() == false) {
            // We don't want to shutdown the threadpool or interrupt threads on a node that is not
            // closed yet.
            throw new IllegalStateException("Call close() first");
        }

        ThreadPool threadPool = injector.getInstance(ThreadPool.class);
        final boolean terminated = ThreadPool.terminate(threadPool, timeout, timeUnit);
        if (terminated) {
            // All threads terminated successfully. Because search, recovery and all other operations
            // that run on shards run in the threadpool, indices should be effectively closed by now.
            if (nodeService.awaitClose(0, TimeUnit.MILLISECONDS) == false) {
                throw new IllegalStateException(
                    "Some shards are still open after the threadpool terminated. "
                        + "Something is leaking index readers or store references."
                );
            }
        }
        return terminated;
    }

    /**
     * Returns {@code true} if the node is closed.
     */
    public boolean isClosed() {
        return lifecycle.closed();
    }

    public Injector injector() {
        return this.injector;
    }

    /**
     * Hook for validating the node after network
     * services are started but before the cluster service is started
     * and before the network service starts accepting incoming network
     * requests.
     *
     * @param context               the bootstrap context for this node
     * @param boundTransportAddress the network addresses the node is
     *                              bound and publishing to
     */
    @SuppressWarnings("unused")
    protected void validateNodeBeforeAcceptingRequests(
        final BootstrapContext context,
        final BoundTransportAddress boundTransportAddress,
        List<BootstrapCheck> bootstrapChecks
    ) throws NodeValidationException {}

    /** Writes a file to the logs dir containing the ports for the given transport type */
    private void writePortsFile(String type, BoundTransportAddress boundAddress) {
        Path tmpPortsFile = environment.logsDir().resolve(type + ".ports.tmp");
        try (BufferedWriter writer = Files.newBufferedWriter(tmpPortsFile, StandardCharsets.UTF_8)) {
            for (TransportAddress address : boundAddress.boundAddresses()) {
                InetAddress inetAddress = InetAddress.getByName(address.getAddress());
                writer.write(NetworkAddress.format(new InetSocketAddress(inetAddress, address.getPort())) + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write ports file", e);
        }
        Path portsFile = environment.logsDir().resolve(type + ".ports");
        try {
            Files.move(tmpPortsFile, portsFile, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rename ports file", e);
        }
    }

    /**
     * The {@link PluginsService} used to build this node's components.
     */
    protected PluginsService getPluginsService() {
        return pluginsService;
    }

    /**
     * Creates a new {@link CircuitBreakerService} based on the settings provided.
     * @see #BREAKER_TYPE_KEY
     */
    public static CircuitBreakerService createCircuitBreakerService(
        Settings settings,
        List<BreakerSettings> breakerSettings,
        ClusterSettings clusterSettings
    ) {
        String type = BREAKER_TYPE_KEY.get(settings);
        if (type.equals("hierarchy")) {
            return new HierarchyCircuitBreakerService(settings, breakerSettings, clusterSettings);
        } else if (type.equals("none")) {
            return new NoneCircuitBreakerService();
        } else {
            throw new IllegalArgumentException("Unknown circuit breaker type [" + type + "]");
        }
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    BigArrays createBigArrays(PageCacheRecycler pageCacheRecycler, CircuitBreakerService circuitBreakerService) {
        return new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    }

    /**
     * Creates a new {@link BigArrays} instance used for this node.
     * This method can be overwritten by subclasses to change their {@link BigArrays} implementation for instance for testing
     */
    PageCacheRecycler createPageCacheRecycler(Settings settings) {
        return new PageCacheRecycler(settings);
    }

    /**
     * Creates a new the SearchService. This method can be overwritten by tests to inject mock implementations.
     */
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
        return new SearchService(
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

    /**
     * Creates a new the ScriptService. This method can be overwritten by tests to inject mock implementations.
     */
    protected ScriptService newScriptService(Settings settings, Map<String, ScriptEngine> engines, Map<String, ScriptContext<?>> contexts) {
        return new ScriptService(settings, engines, contexts);
    }

    /**
     * Get Custom Name Resolvers list based on a Discovery Plugins list
     * @param discoveryPlugins Discovery plugins list
     */
    private List<NetworkService.CustomNameResolver> getCustomNameResolvers(List<DiscoveryPlugin> discoveryPlugins) {
        List<NetworkService.CustomNameResolver> customNameResolvers = new ArrayList<>();
        for (DiscoveryPlugin discoveryPlugin : discoveryPlugins) {
            NetworkService.CustomNameResolver customNameResolver = discoveryPlugin.getCustomNameResolver(settings());
            if (customNameResolver != null) {
                customNameResolvers.add(customNameResolver);
            }
        }
        return customNameResolvers;
    }

    /** Constructs a ClusterInfoService which may be mocked for tests. */
    protected ClusterInfoService newClusterInfoService(
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        NodeClient client
    ) {
        final InternalClusterInfoService service = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        if (DiscoveryNode.isClusterManagerNode(settings)) {
            // listen for state changes (this node starts/stops being the elected cluster manager, or new nodes are added)
            clusterService.addListener(service);
        }
        return service;
    }

    /** Constructs a {@link org.density.http.HttpServerTransport} which may be mocked for tests. */
    protected HttpServerTransport newHttpTransport(NetworkModule networkModule) {
        return networkModule.getHttpServerTransportSupplier().get();
    }

    protected List<AuxTransport> newAuxTransports(NetworkModule networkModule) {
        return networkModule.getAuxServerTransportList();
    }

    private static class LocalNodeFactory implements Function<BoundTransportAddress, DiscoveryNode> {
        private final SetOnce<DiscoveryNode> localNode = new SetOnce<>();
        private final String persistentNodeId;
        private final Settings settings;
        private final RemoteStoreNodeService remoteStoreNodeService;

        private LocalNodeFactory(Settings settings, String persistentNodeId, RemoteStoreNodeService remoteStoreNodeService) {
            this.persistentNodeId = persistentNodeId;
            this.settings = settings;
            this.remoteStoreNodeService = remoteStoreNodeService;
        }

        @Override
        public DiscoveryNode apply(BoundTransportAddress boundTransportAddress) {
            final DiscoveryNode discoveryNode = DiscoveryNode.createLocal(
                settings,
                boundTransportAddress.publishAddress(),
                persistentNodeId
            );

            if (isRemoteStoreAttributePresent(settings)) {
                remoteStoreNodeService.createAndVerifyRepositories(discoveryNode);
            }

            localNode.set(discoveryNode);
            return localNode.get();
        }

        DiscoveryNode getNode() {
            assert localNode.get() != null;
            return localNode.get();
        }
    }

    /**
     * Initializes the warm cache with a defined capacity.
     * The capacity of the cache is based on user configuration for {@link Node#NODE_SEARCH_CACHE_SIZE_SETTING}.
     * If the user doesn't configure the cache size, it fails if the node is a data + warm node.
     * Else it configures the size to 80% of total capacity for a dedicated warm node, if not explicitly defined.
     */
    private void initializeFileCache(Settings settings, CircuitBreaker circuitBreaker) throws IOException {
        if (DiscoveryNode.isWarmNode(settings) == false) {
            return;
        }

        String capacityRaw = NODE_SEARCH_CACHE_SIZE_SETTING.get(settings);
        logger.info("cache size [{}]", capacityRaw);
        if (capacityRaw.equals(ZERO)) {
            throw new SettingsException(
                "Unable to initialize the "
                    + DiscoveryNodeRole.WARM_ROLE.roleName()
                    + "-"
                    + DiscoveryNodeRole.DATA_ROLE.roleName()
                    + " node: Missing value for configuration "
                    + NODE_SEARCH_CACHE_SIZE_SETTING.getKey()
            );
        }

        NodeEnvironment.NodePath fileCacheNodePath = nodeEnvironment.fileCacheNodePath();
        long totalSpace = ExceptionsHelper.catchAsRuntimeException(() -> FsProbe.getTotalSize(fileCacheNodePath));
        long capacity = calculateFileCacheSize(capacityRaw, totalSpace);
        if (capacity <= 0 || totalSpace <= capacity) {
            throw new SettingsException("Cache size must be larger than zero and less than total capacity");
        }

        this.fileCache = FileCacheFactory.createConcurrentLRUFileCache(capacity, circuitBreaker);
        fileCacheNodePath.fileCacheReservedSize = new ByteSizeValue(this.fileCache.capacity(), ByteSizeUnit.BYTES);
        ForkJoinPool loadFileCacheThreadpool = new ForkJoinPool(
            Runtime.getRuntime().availableProcessors(),
            Node.CustomForkJoinWorkerThread::new,
            null,
            false
        );
        SetOnce<UncheckedIOException> exception = new SetOnce<>();
        ForkJoinTask<Void> fileCacheFilesLoadTask = loadFileCacheThreadpool.submit(
            new FileCache.LoadTask(fileCacheNodePath.fileCachePath, this.fileCache, exception)
        );
        if (DiscoveryNode.isDedicatedWarmNode(settings)) {
            ForkJoinTask<Void> indicesFilesLoadTask = loadFileCacheThreadpool.submit(
                new FileCache.LoadTask(fileCacheNodePath.indicesPath, this.fileCache, exception)
            );
            indicesFilesLoadTask.join();
        }
        fileCacheFilesLoadTask.join();
        loadFileCacheThreadpool.shutdown();
        if (exception.get() != null) {
            logger.error("File cache initialization failed.", exception.get());
            throw new DensityException(exception.get());
        }
    }

    /**
     * Custom ForkJoinWorkerThread that preserves the context ClassLoader of the creating thread
     * to ensure proper resource loading in worker threads.
     */
    public static class CustomForkJoinWorkerThread extends ForkJoinWorkerThread {
        public CustomForkJoinWorkerThread(ForkJoinPool pool) {
            super(pool);
            setContextClassLoader(Thread.currentThread().getContextClassLoader());
        }
    }

    private static long calculateFileCacheSize(String capacityRaw, long totalSpace) {
        try {
            RatioValue ratioValue = RatioValue.parseRatioValue(capacityRaw);
            return Math.round(totalSpace * ratioValue.getAsRatio());
        } catch (DensityParseException e) {
            try {
                return ByteSizeValue.parseBytesSizeValue(capacityRaw, NODE_SEARCH_CACHE_SIZE_SETTING.getKey()).getBytes();
            } catch (DensityParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
    }

    private static String validateFileCacheSize(String capacityRaw) {
        calculateFileCacheSize(capacityRaw, 0L);
        return capacityRaw;
    }

    /**
     * Returns the {@link FileCache} instance for remote warm node
     * Note: Visible for testing
     */
    public FileCache fileCache() {
        return this.fileCache;
    }
}
