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

import org.density.Build;
import org.density.Version;
import org.density.action.admin.cluster.node.info.NodeInfo;
import org.density.action.admin.cluster.node.stats.NodeStats;
import org.density.action.admin.indices.stats.CommonStatsFlags;
import org.density.action.search.SearchTransportService;
import org.density.cluster.routing.WeightedRoutingStats;
import org.density.cluster.service.ClusterService;
import org.density.common.Nullable;
import org.density.common.cache.service.CacheService;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.common.util.io.IOUtils;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.discovery.Discovery;
import org.density.http.HttpServerTransport;
import org.density.index.IndexingPressureService;
import org.density.index.SegmentReplicationStatsTracker;
import org.density.index.store.remote.filecache.FileCache;
import org.density.indices.IndicesService;
import org.density.ingest.IngestService;
import org.density.monitor.MonitorService;
import org.density.node.remotestore.RemoteStoreNodeStats;
import org.density.plugins.PluginsService;
import org.density.ratelimitting.admissioncontrol.AdmissionControlService;
import org.density.repositories.RepositoriesService;
import org.density.script.ScriptService;
import org.density.search.aggregations.support.AggregationUsageService;
import org.density.search.backpressure.SearchBackpressureService;
import org.density.search.pipeline.SearchPipelineService;
import org.density.tasks.TaskCancellationMonitoringService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Services exposed to nodes
 *
 * @density.internal
 */
public class NodeService implements Closeable {
    private final Settings settings;
    private final ThreadPool threadPool;
    private final MonitorService monitorService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final PluginsService pluginService;
    private final CircuitBreakerService circuitBreakerService;
    private final IngestService ingestService;
    private final SettingsFilter settingsFilter;
    private final ScriptService scriptService;
    private final HttpServerTransport httpServerTransport;
    private final ResponseCollectorService responseCollectorService;
    private final ResourceUsageCollectorService resourceUsageCollectorService;
    private final SearchTransportService searchTransportService;
    private final IndexingPressureService indexingPressureService;
    private final AggregationUsageService aggregationUsageService;
    private final SearchBackpressureService searchBackpressureService;
    private final SearchPipelineService searchPipelineService;
    private final ClusterService clusterService;
    private final Discovery discovery;
    private final FileCache fileCache;
    private final TaskCancellationMonitoringService taskCancellationMonitoringService;
    private final RepositoriesService repositoriesService;
    private final AdmissionControlService admissionControlService;
    private final SegmentReplicationStatsTracker segmentReplicationStatsTracker;
    private final CacheService cacheService;

    NodeService(
        Settings settings,
        ThreadPool threadPool,
        MonitorService monitorService,
        Discovery discovery,
        TransportService transportService,
        IndicesService indicesService,
        PluginsService pluginService,
        CircuitBreakerService circuitBreakerService,
        ScriptService scriptService,
        @Nullable HttpServerTransport httpServerTransport,
        IngestService ingestService,
        ClusterService clusterService,
        SettingsFilter settingsFilter,
        ResponseCollectorService responseCollectorService,
        SearchTransportService searchTransportService,
        IndexingPressureService indexingPressureService,
        AggregationUsageService aggregationUsageService,
        SearchBackpressureService searchBackpressureService,
        SearchPipelineService searchPipelineService,
        FileCache fileCache,
        TaskCancellationMonitoringService taskCancellationMonitoringService,
        ResourceUsageCollectorService resourceUsageCollectorService,
        SegmentReplicationStatsTracker segmentReplicationStatsTracker,
        RepositoriesService repositoriesService,
        AdmissionControlService admissionControlService,
        CacheService cacheService
    ) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.monitorService = monitorService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.discovery = discovery;
        this.pluginService = pluginService;
        this.circuitBreakerService = circuitBreakerService;
        this.httpServerTransport = httpServerTransport;
        this.ingestService = ingestService;
        this.settingsFilter = settingsFilter;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.searchTransportService = searchTransportService;
        this.indexingPressureService = indexingPressureService;
        this.aggregationUsageService = aggregationUsageService;
        this.searchBackpressureService = searchBackpressureService;
        this.searchPipelineService = searchPipelineService;
        this.clusterService = clusterService;
        this.fileCache = fileCache;
        this.taskCancellationMonitoringService = taskCancellationMonitoringService;
        this.resourceUsageCollectorService = resourceUsageCollectorService;
        this.repositoriesService = repositoriesService;
        this.admissionControlService = admissionControlService;
        clusterService.addStateApplier(ingestService);
        clusterService.addStateApplier(searchPipelineService);
        this.segmentReplicationStatsTracker = segmentReplicationStatsTracker;
        this.cacheService = cacheService;
    }

    public NodeInfo info(
        boolean settings,
        boolean os,
        boolean process,
        boolean jvm,
        boolean threadPool,
        boolean transport,
        boolean http,
        boolean plugin,
        boolean ingest,
        boolean aggs,
        boolean indices,
        boolean searchPipeline
    ) {
        NodeInfo.Builder builder = NodeInfo.builder(Version.CURRENT, Build.CURRENT, transportService.getLocalNode());
        if (settings) {
            builder.setSettings(settingsFilter.filter(this.settings));
        }
        if (os) {
            builder.setOs(monitorService.osService().info());
        }
        if (process) {
            builder.setProcess(monitorService.processService().info());
        }
        if (jvm) {
            builder.setJvm(monitorService.jvmService().info());
        }
        if (threadPool) {
            builder.setThreadPool(this.threadPool.info());
        }
        if (transport) {
            builder.setTransport(transportService.info());
        }
        if (http && httpServerTransport != null) {
            builder.setHttp(httpServerTransport.info());
        }
        if (plugin && pluginService != null) {
            builder.setPlugins(pluginService.info());
        }
        if (ingest && ingestService != null) {
            builder.setIngest(ingestService.info());
        }
        if (aggs && aggregationUsageService != null) {
            builder.setAggsInfo(aggregationUsageService.info());
        }
        if (indices) {
            builder.setTotalIndexingBuffer(indicesService.getTotalIndexingBufferBytes());
        }
        if (searchPipeline && searchPipelineService != null) {
            builder.setSearchPipelineInfo(searchPipelineService.info());
        }
        return builder.build();
    }

    public NodeStats stats(
        CommonStatsFlags indices,
        boolean os,
        boolean process,
        boolean jvm,
        boolean threadPool,
        boolean fs,
        boolean transport,
        boolean http,
        boolean circuitBreaker,
        boolean script,
        boolean discoveryStats,
        boolean ingest,
        boolean adaptiveSelection,
        boolean scriptCache,
        boolean indexingPressure,
        boolean shardIndexingPressure,
        boolean searchBackpressure,
        boolean clusterManagerThrottling,
        boolean weightedRoutingStats,
        boolean fileCacheStats,
        boolean taskCancellation,
        boolean searchPipelineStats,
        boolean resourceUsageStats,
        boolean segmentReplicationTrackerStats,
        boolean repositoriesStats,
        boolean admissionControl,
        boolean cacheService,
        boolean remoteStoreNodeStats
    ) {
        // for indices stats we want to include previous allocated shards stats as well (it will
        // only be applied to the sensible ones to use, like refresh/merge/flush/indexing stats)
        return new NodeStats(
            transportService.getLocalNode(),
            System.currentTimeMillis(),
            indices.anySet() ? indicesService.stats(indices) : null,
            os ? monitorService.osService().stats() : null,
            process ? monitorService.processService().stats() : null,
            jvm ? monitorService.jvmService().stats() : null,
            threadPool ? this.threadPool.stats() : null,
            fs ? monitorService.fsService().stats() : null,
            transport ? transportService.stats() : null,
            http ? (httpServerTransport == null ? null : httpServerTransport.stats()) : null,
            circuitBreaker ? circuitBreakerService.stats() : null,
            script ? scriptService.stats() : null,
            discoveryStats ? discovery.stats() : null,
            ingest ? ingestService.stats() : null,
            adaptiveSelection ? responseCollectorService.getAdaptiveStats(searchTransportService.getPendingSearchRequests()) : null,
            resourceUsageStats ? resourceUsageCollectorService.stats() : null,
            scriptCache ? scriptService.cacheStats() : null,
            indexingPressure ? this.indexingPressureService.nodeStats() : null,
            shardIndexingPressure ? this.indexingPressureService.shardStats(indices) : null,
            searchBackpressure ? this.searchBackpressureService.nodeStats() : null,
            clusterManagerThrottling ? this.clusterService.getClusterManagerService().getThrottlingStats() : null,
            weightedRoutingStats ? WeightedRoutingStats.getInstance() : null,
            fileCacheStats && fileCache != null ? fileCache.fileCacheStats() : null,
            taskCancellation ? this.taskCancellationMonitoringService.stats() : null,
            searchPipelineStats ? this.searchPipelineService.stats() : null,
            segmentReplicationTrackerStats ? this.segmentReplicationStatsTracker.getTotalRejectionStats() : null,
            repositoriesStats ? this.repositoriesService.getRepositoriesStats() : null,
            admissionControl ? this.admissionControlService.stats() : null,
            cacheService ? this.cacheService.stats(indices) : null,
            remoteStoreNodeStats ? new RemoteStoreNodeStats() : null
        );
    }

    public IngestService getIngestService() {
        return ingestService;
    }

    public MonitorService getMonitorService() {
        return monitorService;
    }

    public SearchBackpressureService getSearchBackpressureService() {
        return searchBackpressureService;
    }

    public TaskCancellationMonitoringService getTaskCancellationMonitoringService() {
        return taskCancellationMonitoringService;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(indicesService);
    }

    /**
     * Wait for the node to be effectively closed.
     * @see IndicesService#awaitClose(long, TimeUnit)
     */
    public boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return indicesService.awaitClose(timeout, timeUnit);
    }

}
