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

import org.density.Version;
import org.density.action.admin.cluster.node.info.NodeInfo;
import org.density.action.admin.cluster.node.stats.NodeStats;
import org.density.action.admin.indices.stats.CommonStats;
import org.density.action.admin.indices.stats.ShardStats;
import org.density.action.support.nodes.BaseNodeResponse;
import org.density.cluster.health.ClusterHealthStatus;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.Nullable;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.index.cache.query.QueryCacheStats;
import org.density.index.engine.SegmentsStats;
import org.density.index.fielddata.FieldDataStats;
import org.density.index.shard.DocsStats;
import org.density.index.store.StoreStats;
import org.density.search.suggest.completion.CompletionStats;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Transport action for obtaining cluster stats from node level
 *
 * @density.internal
 */
public class ClusterStatsNodeResponse extends BaseNodeResponse {

    private final NodeInfo nodeInfo;
    private final NodeStats nodeStats;
    private final ShardStats[] shardsStats;
    private ClusterHealthStatus clusterStatus;
    private AggregatedNodeLevelStats aggregatedNodeLevelStats;

    public ClusterStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        clusterStatus = null;
        if (in.readBoolean()) {
            clusterStatus = ClusterHealthStatus.fromValue(in.readByte());
        }
        this.nodeInfo = new NodeInfo(in);
        this.nodeStats = new NodeStats(in);
        if (in.getVersion().onOrAfter(Version.V_2_16_0)) {
            this.shardsStats = in.readOptionalArray(ShardStats::new, ShardStats[]::new);
            this.aggregatedNodeLevelStats = in.readOptionalWriteable(AggregatedNodeLevelStats::new);
        } else {
            this.shardsStats = in.readArray(ShardStats::new, ShardStats[]::new);
        }
    }

    public ClusterStatsNodeResponse(
        DiscoveryNode node,
        @Nullable ClusterHealthStatus clusterStatus,
        NodeInfo nodeInfo,
        NodeStats nodeStats,
        ShardStats[] shardsStats
    ) {
        super(node);
        this.nodeInfo = nodeInfo;
        this.nodeStats = nodeStats;
        this.shardsStats = shardsStats;
        this.clusterStatus = clusterStatus;
    }

    public ClusterStatsNodeResponse(
        DiscoveryNode node,
        @Nullable ClusterHealthStatus clusterStatus,
        NodeInfo nodeInfo,
        NodeStats nodeStats,
        ShardStats[] shardsStats,
        boolean useAggregatedNodeLevelResponses
    ) {
        super(node);
        this.nodeInfo = nodeInfo;
        this.nodeStats = nodeStats;
        if (useAggregatedNodeLevelResponses) {
            this.aggregatedNodeLevelStats = new AggregatedNodeLevelStats(node, shardsStats);
        }
        this.shardsStats = shardsStats;
        this.clusterStatus = clusterStatus;
    }

    public NodeInfo nodeInfo() {
        return this.nodeInfo;
    }

    public NodeStats nodeStats() {
        return this.nodeStats;
    }

    /**
     * Cluster Health Status, only populated on cluster-manager nodes.
     */
    @Nullable
    public ClusterHealthStatus clusterStatus() {
        return clusterStatus;
    }

    public ShardStats[] shardsStats() {
        return this.shardsStats;
    }

    public AggregatedNodeLevelStats getAggregatedNodeLevelStats() {
        return aggregatedNodeLevelStats;
    }

    public static ClusterStatsNodeResponse readNodeResponse(StreamInput in) throws IOException {
        return new ClusterStatsNodeResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (clusterStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByte(clusterStatus.value());
        }
        nodeInfo.writeTo(out);
        nodeStats.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_16_0)) {
            if (aggregatedNodeLevelStats != null) {
                out.writeOptionalArray(null);
                out.writeOptionalWriteable(aggregatedNodeLevelStats);
            } else {
                out.writeOptionalArray(shardsStats);
                out.writeOptionalWriteable(null);
            }
        } else {
            out.writeArray(shardsStats);
        }
    }

    /**
     * Node level statistics used for ClusterStatsIndices for _cluster/stats call.
     */
    public class AggregatedNodeLevelStats extends BaseNodeResponse {

        CommonStats commonStats;
        Map<String, AggregatedIndexStats> indexStatsMap;

        protected AggregatedNodeLevelStats(StreamInput in) throws IOException {
            super(in);
            commonStats = in.readOptionalWriteable(CommonStats::new);
            indexStatsMap = in.readMap(StreamInput::readString, AggregatedIndexStats::new);
        }

        protected AggregatedNodeLevelStats(DiscoveryNode node, ShardStats[] indexShardsStats) {
            super(node);
            this.commonStats = new CommonStats();
            this.commonStats.docs = new DocsStats();
            this.commonStats.store = new StoreStats();
            this.commonStats.fieldData = new FieldDataStats();
            this.commonStats.queryCache = new QueryCacheStats();
            this.commonStats.completion = new CompletionStats();
            this.commonStats.segments = new SegmentsStats();
            this.indexStatsMap = new HashMap<>();

            // Index Level Stats
            for (org.density.action.admin.indices.stats.ShardStats shardStats : indexShardsStats) {
                AggregatedIndexStats indexShardStats = this.indexStatsMap.get(shardStats.getShardRouting().getIndexName());
                if (indexShardStats == null) {
                    indexShardStats = new AggregatedIndexStats();
                    this.indexStatsMap.put(shardStats.getShardRouting().getIndexName(), indexShardStats);
                }

                indexShardStats.total++;

                CommonStats shardCommonStats = shardStats.getStats();

                if (shardStats.getShardRouting().primary()) {
                    indexShardStats.primaries++;
                    this.commonStats.docs.add(shardCommonStats.docs);
                }
                this.commonStats.store.add(shardCommonStats.store);
                this.commonStats.fieldData.add(shardCommonStats.fieldData);
                this.commonStats.queryCache.add(shardCommonStats.queryCache);
                this.commonStats.completion.add(shardCommonStats.completion);
                this.commonStats.segments.add(shardCommonStats.segments);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(commonStats);
            out.writeMap(indexStatsMap, StreamOutput::writeString, (stream, stats) -> stats.writeTo(stream));
        }
    }

    /**
     * Node level statistics used for ClusterStatsIndices for _cluster/stats call.
     */
    @PublicApi(since = "2.16.0")
    public static class AggregatedIndexStats implements Writeable {
        public int total = 0;
        public int primaries = 0;

        public AggregatedIndexStats(StreamInput in) throws IOException {
            total = in.readVInt();
            primaries = in.readVInt();
        }

        public AggregatedIndexStats() {}

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(primaries);
        }
    }
}
