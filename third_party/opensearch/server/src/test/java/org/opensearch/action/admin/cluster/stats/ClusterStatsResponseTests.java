/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.stats;

import org.density.Build;
import org.density.Version;
import org.density.action.admin.cluster.node.info.NodeInfo;
import org.density.action.admin.cluster.node.info.PluginsAndModules;
import org.density.action.admin.cluster.node.stats.NodeStats;
import org.density.action.admin.cluster.stats.ClusterStatsRequest.IndexMetric;
import org.density.action.admin.indices.stats.CommonStats;
import org.density.action.admin.indices.stats.CommonStatsFlags;
import org.density.action.admin.indices.stats.ShardStats;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.routing.ShardRoutingState;
import org.density.cluster.routing.TestShardRouting;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.settings.Settings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.transport.BoundTransportAddress;
import org.density.core.common.transport.TransportAddress;
import org.density.core.index.Index;
import org.density.index.cache.query.QueryCacheStats;
import org.density.index.engine.SegmentsStats;
import org.density.index.fielddata.FieldDataStats;
import org.density.index.flush.FlushStats;
import org.density.index.shard.DocsStats;
import org.density.index.shard.IndexingStats;
import org.density.index.shard.ShardPath;
import org.density.index.store.StoreStats;
import org.density.monitor.jvm.JvmInfo;
import org.density.monitor.jvm.JvmStats;
import org.density.monitor.os.OsInfo;
import org.density.monitor.process.ProcessStats;
import org.density.search.suggest.completion.CompletionStats;
import org.density.test.DensityTestCase;
import org.density.transport.TransportInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class ClusterStatsResponseTests extends DensityTestCase {

    public void testSerializationWithIndicesMappingAndAnalysisStats() throws Exception {
        List<ClusterStatsNodeResponse> defaultClusterStatsNodeResponses = new ArrayList<>();

        int numberOfNodes = randomIntBetween(1, 4);
        Index testIndex = new Index("test-index", "_na_");

        for (int i = 0; i < numberOfNodes; i++) {
            DiscoveryNode node = new DiscoveryNode("node-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
            CommonStats commonStats = createRandomCommonStats();
            ShardStats[] shardStats = createShardStats(node, testIndex, commonStats);
            ClusterStatsNodeResponse customClusterStatsResponse = createClusterStatsNodeResponse(node, shardStats);
            defaultClusterStatsNodeResponses.add(customClusterStatsResponse);
        }
        ClusterStatsResponse clusterStatsResponse = new ClusterStatsResponse(
            1l,
            "UUID",
            new ClusterName("cluster_name"),
            defaultClusterStatsNodeResponses,
            List.of(),
            ClusterState.EMPTY_STATE,
            Set.of(ClusterStatsRequest.Metric.INDICES),
            Set.of(IndexMetric.MAPPINGS, IndexMetric.ANALYSIS)
        );
        BytesStreamOutput output = new BytesStreamOutput();
        clusterStatsResponse.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ClusterStatsResponse deserializedClusterStatsResponse = new ClusterStatsResponse(streamInput);
        assertEquals(clusterStatsResponse.timestamp, deserializedClusterStatsResponse.timestamp);
        assertEquals(clusterStatsResponse.status, deserializedClusterStatsResponse.status);
        assertEquals(clusterStatsResponse.clusterUUID, deserializedClusterStatsResponse.clusterUUID);
        assertNotNull(clusterStatsResponse.indicesStats);
        assertEquals(clusterStatsResponse.indicesStats.getMappings(), deserializedClusterStatsResponse.indicesStats.getMappings());
        assertEquals(clusterStatsResponse.indicesStats.getAnalysis(), deserializedClusterStatsResponse.indicesStats.getAnalysis());
    }

    public void testSerializationWithoutIndicesMappingAndAnalysisStats() throws Exception {
        List<ClusterStatsNodeResponse> defaultClusterStatsNodeResponses = new ArrayList<>();

        int numberOfNodes = randomIntBetween(1, 4);
        Index testIndex = new Index("test-index", "_na_");

        for (int i = 0; i < numberOfNodes; i++) {
            DiscoveryNode node = new DiscoveryNode("node-" + i, buildNewFakeTransportAddress(), Version.CURRENT);
            CommonStats commonStats = createRandomCommonStats();
            ShardStats[] shardStats = createShardStats(node, testIndex, commonStats);
            ClusterStatsNodeResponse customClusterStatsResponse = createClusterStatsNodeResponse(node, shardStats);
            defaultClusterStatsNodeResponses.add(customClusterStatsResponse);
        }
        ClusterStatsResponse clusterStatsResponse = new ClusterStatsResponse(
            1l,
            "UUID",
            new ClusterName("cluster_name"),
            defaultClusterStatsNodeResponses,
            List.of(),
            ClusterState.EMPTY_STATE,
            Set.of(ClusterStatsRequest.Metric.INDICES, ClusterStatsRequest.Metric.PROCESS, ClusterStatsRequest.Metric.JVM),
            Set.of(
                IndexMetric.DOCS,
                IndexMetric.STORE,
                IndexMetric.SEGMENTS,
                IndexMetric.QUERY_CACHE,
                IndexMetric.FIELDDATA,
                IndexMetric.COMPLETION
            )
        );
        BytesStreamOutput output = new BytesStreamOutput();
        clusterStatsResponse.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ClusterStatsResponse deserializedClusterStatsResponse = new ClusterStatsResponse(streamInput);
        assertEquals(clusterStatsResponse.timestamp, deserializedClusterStatsResponse.timestamp);
        assertEquals(clusterStatsResponse.status, deserializedClusterStatsResponse.status);
        assertEquals(clusterStatsResponse.clusterUUID, deserializedClusterStatsResponse.clusterUUID);
        assertNotNull(deserializedClusterStatsResponse.nodesStats);
        assertNotNull(deserializedClusterStatsResponse.nodesStats.getProcess());
        assertNotNull(deserializedClusterStatsResponse.nodesStats.getJvm());
        assertNotNull(deserializedClusterStatsResponse.indicesStats);
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getDocs());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getStore());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getSegments());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getQueryCache());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getFieldData());
        assertNotNull(deserializedClusterStatsResponse.indicesStats.getCompletion());
        assertNull(deserializedClusterStatsResponse.indicesStats.getMappings());
        assertNull(deserializedClusterStatsResponse.indicesStats.getAnalysis());
    }

    private ClusterStatsNodeResponse createClusterStatsNodeResponse(DiscoveryNode node, ShardStats[] shardStats) throws IOException {
        JvmStats.GarbageCollector[] garbageCollectorsArray = new JvmStats.GarbageCollector[1];
        garbageCollectorsArray[0] = new JvmStats.GarbageCollector(
            randomAlphaOfLengthBetween(3, 10),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        JvmStats.GarbageCollectors garbageCollectors = new JvmStats.GarbageCollectors(garbageCollectorsArray);
        NodeInfo nodeInfo = new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            node,
            Settings.EMPTY,
            new OsInfo(randomLong(), randomInt(), randomInt(), "name", "pretty_name", "arch", "version"),
            null,
            JvmInfo.jvmInfo(),
            null,
            new TransportInfo(
                new BoundTransportAddress(new TransportAddress[] { buildNewFakeTransportAddress() }, buildNewFakeTransportAddress()),
                null
            ),
            null,
            new PluginsAndModules(Collections.emptyList(), Collections.emptyList()),
            null,
            null,
            null,
            null
        );

        NodeStats nodeStats = new NodeStats(
            node,
            randomNonNegativeLong(),
            null,
            null,
            new ProcessStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                new ProcessStats.Cpu(randomShort(), randomNonNegativeLong()),
                new ProcessStats.Mem(randomNonNegativeLong())
            ),
            new JvmStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                new JvmStats.Mem(
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    Collections.emptyList()
                ),
                new JvmStats.Threads(randomIntBetween(1, 1000), randomIntBetween(1, 1000)),
                garbageCollectors,
                Collections.emptyList(),
                new JvmStats.Classes(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
            ),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        return new ClusterStatsNodeResponse(node, null, nodeInfo, nodeStats, shardStats);

    }

    private CommonStats createRandomCommonStats() {
        CommonStats commonStats = new CommonStats(CommonStatsFlags.NONE);
        commonStats.docs = new DocsStats(randomLongBetween(0, 10000), randomLongBetween(0, 100), randomLongBetween(0, 1000));
        commonStats.store = new StoreStats(randomLongBetween(0, 100), randomLongBetween(0, 1000));
        commonStats.indexing = new IndexingStats();
        commonStats.completion = new CompletionStats();
        commonStats.flush = new FlushStats(randomLongBetween(0, 100), randomLongBetween(0, 100), randomLongBetween(0, 100));
        commonStats.fieldData = new FieldDataStats(randomLongBetween(0, 100), randomLongBetween(0, 100), null);
        commonStats.queryCache = new QueryCacheStats(
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100)
        );
        commonStats.segments = new SegmentsStats();

        return commonStats;
    }

    private ShardStats[] createShardStats(DiscoveryNode localNode, Index index, CommonStats commonStats) {
        List<ShardStats> shardStatsList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            ShardRoutingState shardRoutingState = ShardRoutingState.fromValue((byte) randomIntBetween(2, 3));
            ShardRouting shardRouting = TestShardRouting.newShardRouting(
                index.getName(),
                i,
                localNode.getId(),
                randomBoolean(),
                shardRoutingState
            );

            Path path = createTempDir().resolve("indices")
                .resolve(shardRouting.shardId().getIndex().getUUID())
                .resolve(String.valueOf(shardRouting.shardId().id()));

            ShardStats shardStats = new ShardStats(
                shardRouting,
                new ShardPath(false, path, path, shardRouting.shardId()),
                commonStats,
                null,
                null,
                null,
                null
            );
            shardStatsList.add(shardStats);
        }

        return shardStatsList.toArray(new ShardStats[0]);
    }

}
