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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.action.admin.cluster.stats;

import org.apache.lucene.store.AlreadyClosedException;
import org.density.action.FailedNodeException;
import org.density.action.admin.cluster.health.ClusterHealthRequest;
import org.density.action.admin.cluster.node.info.NodeInfo;
import org.density.action.admin.cluster.node.stats.NodeStats;
import org.density.action.admin.cluster.stats.ClusterStatsRequest.Metric;
import org.density.action.admin.indices.stats.CommonStats;
import org.density.action.admin.indices.stats.CommonStatsFlags;
import org.density.action.admin.indices.stats.ShardStats;
import org.density.action.support.ActionFilters;
import org.density.action.support.nodes.TransportNodesAction;
import org.density.cluster.ClusterState;
import org.density.cluster.health.ClusterHealthStatus;
import org.density.cluster.health.ClusterStateHealth;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.index.IndexService;
import org.density.index.engine.CommitStats;
import org.density.index.seqno.RetentionLeaseStats;
import org.density.index.seqno.SeqNoStats;
import org.density.index.shard.IndexShard;
import org.density.indices.IndicesService;
import org.density.indices.pollingingest.PollingIngestStats;
import org.density.node.NodeService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportRequest;
import org.density.transport.TransportService;
import org.density.transport.Transports;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transport action for obtaining cluster state
 *
 * @density.internal
 */
public class TransportClusterStatsAction extends TransportNodesAction<
    ClusterStatsRequest,
    ClusterStatsResponse,
    TransportClusterStatsAction.ClusterStatsNodeRequest,
    ClusterStatsNodeResponse> {

    private static final Map<CommonStatsFlags.Flag, ClusterStatsRequest.IndexMetric> SHARDS_STATS_FLAG_MAP_TO_INDEX_METRIC = Map.of(
        CommonStatsFlags.Flag.Docs,
        ClusterStatsRequest.IndexMetric.DOCS,
        CommonStatsFlags.Flag.Store,
        ClusterStatsRequest.IndexMetric.STORE,
        CommonStatsFlags.Flag.FieldData,
        ClusterStatsRequest.IndexMetric.FIELDDATA,
        CommonStatsFlags.Flag.QueryCache,
        ClusterStatsRequest.IndexMetric.QUERY_CACHE,
        CommonStatsFlags.Flag.Completion,
        ClusterStatsRequest.IndexMetric.COMPLETION,
        CommonStatsFlags.Flag.Segments,
        ClusterStatsRequest.IndexMetric.SEGMENTS
    );

    private final NodeService nodeService;
    private final IndicesService indicesService;

    @Inject
    public TransportClusterStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        NodeService nodeService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(
            ClusterStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ClusterStatsRequest::new,
            ClusterStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            ThreadPool.Names.MANAGEMENT,
            ClusterStatsNodeResponse.class
        );
        this.nodeService = nodeService;
        this.indicesService = indicesService;
    }

    @Override
    protected ClusterStatsResponse newResponse(
        ClusterStatsRequest request,
        List<ClusterStatsNodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        assert Transports.assertNotTransportThread(
            "Constructor of ClusterStatsResponse runs expensive computations on mappings found in"
                + " the cluster state that are too slow for a transport thread"
        );
        ClusterState state = clusterService.state();
        if (request.computeAllMetrics()) {
            return new ClusterStatsResponse(
                System.currentTimeMillis(),
                state.metadata().clusterUUID(),
                clusterService.getClusterName(),
                responses,
                failures,
                state
            );
        } else {
            return new ClusterStatsResponse(
                System.currentTimeMillis(),
                state.metadata().clusterUUID(),
                clusterService.getClusterName(),
                responses,
                failures,
                state,
                request.requestedMetrics(),
                request.indicesMetrics()
            );
        }
    }

    @Override
    protected ClusterStatsNodeRequest newNodeRequest(ClusterStatsRequest request) {
        return new ClusterStatsNodeRequest(request);
    }

    @Override
    protected ClusterStatsNodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new ClusterStatsNodeResponse(in);
    }

    @Override
    protected ClusterStatsNodeResponse nodeOperation(ClusterStatsNodeRequest nodeRequest) {
        NodeInfo nodeInfo = nodeService.info(true, true, false, true, false, true, false, true, false, false, false, false);
        NodeStats nodeStats = nodeService.stats(
            CommonStatsFlags.NONE,
            isMetricRequired(Metric.OS, nodeRequest.request),
            isMetricRequired(Metric.PROCESS, nodeRequest.request),
            isMetricRequired(Metric.JVM, nodeRequest.request),
            false,
            isMetricRequired(Metric.FS, nodeRequest.request),
            false,
            false,
            false,
            false,
            false,
            isMetricRequired(Metric.INGEST, nodeRequest.request),
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false,
            false
        );
        List<ShardStats> shardsStats = new ArrayList<>();
        if (isMetricRequired(Metric.INDICES, nodeRequest.request)) {
            CommonStatsFlags commonStatsFlags = getCommonStatsFlags(nodeRequest);
            for (IndexService indexService : indicesService) {
                for (IndexShard indexShard : indexService) {
                    if (indexShard.routingEntry() != null && indexShard.routingEntry().active()) {
                        // only report on fully started shards
                        CommitStats commitStats;
                        SeqNoStats seqNoStats;
                        RetentionLeaseStats retentionLeaseStats;
                        PollingIngestStats pollingIngestStats;
                        try {
                            commitStats = indexShard.commitStats();
                            seqNoStats = indexShard.seqNoStats();
                            retentionLeaseStats = indexShard.getRetentionLeaseStats();
                            pollingIngestStats = indexShard.pollingIngestStats();
                        } catch (final AlreadyClosedException e) {
                            // shard is closed - no stats is fine
                            commitStats = null;
                            seqNoStats = null;
                            retentionLeaseStats = null;
                            pollingIngestStats = null;
                        }
                        shardsStats.add(
                            new ShardStats(
                                indexShard.routingEntry(),
                                indexShard.shardPath(),
                                new CommonStats(indicesService.getIndicesQueryCache(), indexShard, commonStatsFlags),
                                commitStats,
                                seqNoStats,
                                retentionLeaseStats,
                                pollingIngestStats
                            )
                        );
                    }
                }
            }
        }

        ClusterHealthStatus clusterStatus = null;
        if (clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
            clusterStatus = new ClusterStateHealth(clusterService.state(), ClusterHealthRequest.Level.CLUSTER).getStatus();
        }

        return new ClusterStatsNodeResponse(
            nodeInfo.getNode(),
            clusterStatus,
            nodeInfo,
            nodeStats,
            shardsStats.toArray(new ShardStats[0]),
            nodeRequest.request.useAggregatedNodeLevelResponses()
        );
    }

    /**
     * A metric is required when: all cluster stats are required (OR) if the metric is requested
     * @param metric
     * @param clusterStatsRequest
     * @return
     */
    private boolean isMetricRequired(Metric metric, ClusterStatsRequest clusterStatsRequest) {
        return clusterStatsRequest.computeAllMetrics() || clusterStatsRequest.requestedMetrics().contains(metric);
    }

    private static CommonStatsFlags getCommonStatsFlags(ClusterStatsNodeRequest nodeRequest) {
        Set<CommonStatsFlags.Flag> requestedCommonStatsFlags = new HashSet<>();
        if (nodeRequest.request.computeAllMetrics()) {
            requestedCommonStatsFlags.addAll(SHARDS_STATS_FLAG_MAP_TO_INDEX_METRIC.keySet());
        } else {
            for (Map.Entry<CommonStatsFlags.Flag, ClusterStatsRequest.IndexMetric> entry : SHARDS_STATS_FLAG_MAP_TO_INDEX_METRIC
                .entrySet()) {
                if (nodeRequest.request.indicesMetrics().contains(entry.getValue())) {
                    requestedCommonStatsFlags.add(entry.getKey());
                }
            }
        }
        return new CommonStatsFlags(requestedCommonStatsFlags.toArray(new CommonStatsFlags.Flag[0]));
    }

    /**
     * Inner Cluster Stats Node Request
     *
     * @density.internal
     */
    public static class ClusterStatsNodeRequest extends TransportRequest {

        protected ClusterStatsRequest request;

        public ClusterStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new ClusterStatsRequest(in);
        }

        ClusterStatsNodeRequest(ClusterStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
