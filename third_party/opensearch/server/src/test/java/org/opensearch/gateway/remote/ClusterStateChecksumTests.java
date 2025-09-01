/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote;

import org.density.Version;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlock;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.block.ClusterBlocks;
import org.density.cluster.coordination.CoordinationMetadata;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexTemplateMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.TemplatesMetadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.RoutingTable;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.settings.Settings;
import org.density.common.xcontent.json.JsonXContent;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.transport.TransportAddress;
import org.density.core.index.Index;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class ClusterStateChecksumTests extends DensityTestCase {
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @After
    public void teardown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testClusterStateChecksumEmptyClusterState() {
        ClusterStateChecksum checksum = new ClusterStateChecksum(ClusterState.EMPTY_STATE, threadPool);
        assertNotNull(checksum);
    }

    public void testClusterStateChecksum() {
        ClusterStateChecksum checksum = new ClusterStateChecksum(generateClusterState(), threadPool);
        assertNotNull(checksum);
        assertTrue(checksum.routingTableChecksum != 0);
        assertTrue(checksum.nodesChecksum != 0);
        assertTrue(checksum.blocksChecksum != 0);
        assertTrue(checksum.clusterStateCustomsChecksum != 0);
        assertTrue(checksum.coordinationMetadataChecksum != 0);
        assertTrue(checksum.settingMetadataChecksum != 0);
        assertTrue(checksum.transientSettingsMetadataChecksum != 0);
        assertTrue(checksum.templatesMetadataChecksum != 0);
        assertTrue(checksum.customMetadataMapChecksum != 0);
        assertTrue(checksum.hashesOfConsistentSettingsChecksum != 0);
        assertTrue(checksum.indicesChecksum != 0);
        assertTrue(checksum.clusterStateChecksum != 0);
    }

    public void testClusterStateMatchChecksum() {
        ClusterStateChecksum checksum = new ClusterStateChecksum(generateClusterState(), threadPool);
        ClusterStateChecksum newChecksum = new ClusterStateChecksum(generateClusterState(), threadPool);
        assertNotNull(checksum);
        assertNotNull(newChecksum);
        assertEquals(checksum.routingTableChecksum, newChecksum.routingTableChecksum);
        assertEquals(checksum.nodesChecksum, newChecksum.nodesChecksum);
        assertEquals(checksum.blocksChecksum, newChecksum.blocksChecksum);
        assertEquals(checksum.clusterStateCustomsChecksum, newChecksum.clusterStateCustomsChecksum);
        assertEquals(checksum.coordinationMetadataChecksum, newChecksum.coordinationMetadataChecksum);
        assertEquals(checksum.settingMetadataChecksum, newChecksum.settingMetadataChecksum);
        assertEquals(checksum.transientSettingsMetadataChecksum, newChecksum.transientSettingsMetadataChecksum);
        assertEquals(checksum.templatesMetadataChecksum, newChecksum.templatesMetadataChecksum);
        assertEquals(checksum.customMetadataMapChecksum, newChecksum.customMetadataMapChecksum);
        assertEquals(checksum.hashesOfConsistentSettingsChecksum, newChecksum.hashesOfConsistentSettingsChecksum);
        assertEquals(checksum.indicesChecksum, newChecksum.indicesChecksum);
        assertEquals(checksum.clusterStateChecksum, newChecksum.clusterStateChecksum);
    }

    public void testXContentConversion() throws IOException {
        ClusterStateChecksum checksum = new ClusterStateChecksum(generateClusterState(), threadPool);
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        checksum.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final ClusterStateChecksum parsedChecksum = ClusterStateChecksum.fromXContent(parser);
            assertEquals(checksum, parsedChecksum);
        }
    }

    public void testSerialization() throws IOException {
        ClusterStateChecksum checksum = new ClusterStateChecksum(generateClusterState(), threadPool);
        BytesStreamOutput output = new BytesStreamOutput();
        checksum.writeTo(output);

        try (StreamInput in = output.bytes().streamInput()) {
            ClusterStateChecksum deserializedChecksum = new ClusterStateChecksum(in);
            assertEquals(checksum, deserializedChecksum);
        }
    }

    public void testGetMismatchEntities() {
        ClusterState clsState1 = generateClusterState();
        ClusterStateChecksum checksum = new ClusterStateChecksum(clsState1, threadPool);
        assertTrue(checksum.getMismatchEntities(checksum).isEmpty());

        ClusterStateChecksum checksum2 = new ClusterStateChecksum(clsState1, threadPool);
        assertTrue(checksum.getMismatchEntities(checksum2).isEmpty());

        ClusterState clsState2 = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(RoutingTable.builder().build())
            .nodes(DiscoveryNodes.builder().build())
            .blocks(ClusterBlocks.builder().build())
            .customs(Map.of())
            .metadata(Metadata.EMPTY_METADATA)
            .build();
        ClusterStateChecksum checksum3 = new ClusterStateChecksum(clsState2, threadPool);
        List<String> mismatches = checksum.getMismatchEntities(checksum3);
        assertFalse(mismatches.isEmpty());
        assertEquals(11, mismatches.size());
        assertEquals(ClusterStateChecksum.ROUTING_TABLE_CS, mismatches.get(0));
        assertEquals(ClusterStateChecksum.NODES_CS, mismatches.get(1));
        assertEquals(ClusterStateChecksum.BLOCKS_CS, mismatches.get(2));
        assertEquals(ClusterStateChecksum.CUSTOMS_CS, mismatches.get(3));
        assertEquals(ClusterStateChecksum.COORDINATION_MD_CS, mismatches.get(4));
        assertEquals(ClusterStateChecksum.SETTINGS_MD_CS, mismatches.get(5));
        assertEquals(ClusterStateChecksum.TRANSIENT_SETTINGS_MD_CS, mismatches.get(6));
        assertEquals(ClusterStateChecksum.TEMPLATES_MD_CS, mismatches.get(7));
        assertEquals(ClusterStateChecksum.CUSTOM_MD_CS, mismatches.get(8));
        assertEquals(ClusterStateChecksum.HASHES_MD_CS, mismatches.get(9));
        assertEquals(ClusterStateChecksum.INDICES_CS, mismatches.get(10));
    }

    public void testGetMismatchEntitiesUnorderedInput() {
        ClusterState state1 = generateClusterState();
        DiscoveryNode node1 = DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9200), "node1");
        DiscoveryNode node2 = DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9201), "node2");
        DiscoveryNode node3 = DiscoveryNode.createLocal(Settings.EMPTY, new TransportAddress(TransportAddress.META_ADDRESS, 9202), "node3");

        DiscoveryNodes nodes1 = DiscoveryNodes.builder().clusterManagerNodeId("test-node").add(node1).add(node2).add(node3).build();
        DiscoveryNodes nodes2 = DiscoveryNodes.builder().clusterManagerNodeId("test-node").add(node2).add(node3).build();
        nodes2 = nodes2.newNode(node1);
        ClusterState state2 = ClusterState.builder(state1).nodes(nodes1).build();
        ClusterState state3 = ClusterState.builder(state1).nodes(nodes2).build();

        ClusterStateChecksum checksum1 = new ClusterStateChecksum(state2, threadPool);
        ClusterStateChecksum checksum2 = new ClusterStateChecksum(state3, threadPool);
        assertEquals(checksum2, checksum1);
    }

    private ClusterState generateClusterState() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true)
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final Index index2 = new Index("test-index2", "index-uuid2");
        final Settings idxSettings2 = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index2.getUUID())
            .put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true)
            .build();
        final IndexMetadata indexMetadata2 = new IndexMetadata.Builder(index2.getName()).settings(idxSettings2)
            .numberOfShards(3)
            .numberOfReplicas(2)
            .build();
        final CoordinationMetadata coordinationMetadata = CoordinationMetadata.builder().term(1L).build();
        final Settings settings = Settings.builder().put("mock-settings", true).build();
        final TemplatesMetadata templatesMetadata = TemplatesMetadata.builder()
            .put(IndexTemplateMetadata.builder("template1").settings(idxSettings).patterns(List.of("test*")).build())
            .build();
        final RemoteClusterStateTestUtils.CustomMetadata1 customMetadata1 = new RemoteClusterStateTestUtils.CustomMetadata1(
            "custom-metadata-1"
        );
        RemoteClusterStateTestUtils.TestClusterStateCustom1 clusterStateCustom1 = new RemoteClusterStateTestUtils.TestClusterStateCustom1(
            "custom-1"
        );
        return ClusterState.builder(ClusterName.DEFAULT)
            .version(1L)
            .stateUUID("state-uuid")
            .metadata(
                Metadata.builder()
                    .version(1L)
                    .put(indexMetadata, true)
                    .clusterUUID("cluster-uuid")
                    .coordinationMetadata(coordinationMetadata)
                    .persistentSettings(settings)
                    .transientSettings(settings)
                    .templates(templatesMetadata)
                    .hashesOfConsistentSettings(Map.of("key1", "value1", "key2", "value2"))
                    .putCustom(customMetadata1.getWriteableName(), customMetadata1)
                    .indices(Map.of(indexMetadata.getIndex().getName(), indexMetadata, indexMetadata2.getIndex().getName(), indexMetadata2))
                    .build()
            )
            .nodes(DiscoveryNodes.builder().clusterManagerNodeId("test-node").build())
            .blocks(
                ClusterBlocks.builder()
                    .addBlocks(indexMetadata)
                    .addGlobalBlock(new ClusterBlock(1, "block", true, true, true, RestStatus.ACCEPTED, EnumSet.of(ClusterBlockLevel.READ)))
                    .addGlobalBlock(
                        new ClusterBlock(2, "block-name", false, true, true, RestStatus.OK, EnumSet.of(ClusterBlockLevel.WRITE))
                    )
                    .build()
            )
            .customs(Map.of(clusterStateCustom1.getWriteableName(), clusterStateCustom1))
            .routingTable(RoutingTable.builder().addAsNew(indexMetadata).addAsNew(indexMetadata2).version(1L).build())
            .build();
    }
}
