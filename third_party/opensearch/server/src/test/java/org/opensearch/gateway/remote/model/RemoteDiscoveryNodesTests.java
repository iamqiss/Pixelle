/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.Version;
import org.density.cluster.ClusterModule;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.node.DiscoveryNodes;
import org.density.common.blobstore.BlobPath;
import org.density.common.network.NetworkModule;
import org.density.common.remote.BlobPathParameters;
import org.density.core.compress.Compressor;
import org.density.core.compress.NoneCompressor;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.gateway.remote.ClusterMetadataManifest;
import org.density.gateway.remote.RemoteClusterStateUtils;
import org.density.index.remote.RemoteStoreUtils;
import org.density.indices.IndicesModule;
import org.density.test.DensityTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.density.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.density.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_EPHEMERAL_PATH_TOKEN;
import static org.density.gateway.remote.model.RemoteDiscoveryNodes.DISCOVERY_NODES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteDiscoveryNodesTests extends DensityTestCase {
    private static final String TEST_BLOB_NAME = "/test-path/test-blob-name";
    private static final String TEST_BLOB_PATH = "test-path";
    private static final String TEST_BLOB_FILE_NAME = "test-blob-name";
    private static final long METADATA_VERSION = 3L;
    private String clusterUUID;
    private Compressor compressor;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void setup() {
        this.clusterUUID = "test-cluster-uuid";
        compressor = new NoneCompressor();
        namedXContentRegistry = new NamedXContentRegistry(
            Stream.of(
                NetworkModule.getNamedXContents().stream(),
                IndicesModule.getNamedXContents().stream(),
                ClusterModule.getNamedXWriteables().stream()
            ).flatMap(Function.identity()).collect(toList())
        );
    }

    public void testClusterUUID() {
        DiscoveryNodes nodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteObjectForUpload = new RemoteDiscoveryNodes(nodes, METADATA_VERSION, clusterUUID, compressor);
        assertEquals(remoteObjectForUpload.clusterUUID(), clusterUUID);

        RemoteDiscoveryNodes remoteObjectForDownload = new RemoteDiscoveryNodes(TEST_BLOB_NAME, clusterUUID, compressor);
        assertEquals(remoteObjectForDownload.clusterUUID(), clusterUUID);
    }

    public void testFullBlobName() {
        DiscoveryNodes nodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteObjectForUpload = new RemoteDiscoveryNodes(nodes, METADATA_VERSION, clusterUUID, compressor);
        assertNull(remoteObjectForUpload.getFullBlobName());

        RemoteDiscoveryNodes remoteObjectForDownload = new RemoteDiscoveryNodes(TEST_BLOB_NAME, clusterUUID, compressor);
        assertEquals(remoteObjectForDownload.getFullBlobName(), TEST_BLOB_NAME);
    }

    public void testBlobFileName() {
        DiscoveryNodes nodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteObjectForUpload = new RemoteDiscoveryNodes(nodes, METADATA_VERSION, clusterUUID, compressor);
        assertNull(remoteObjectForUpload.getBlobFileName());

        RemoteClusterBlocks remoteObjectForDownload = new RemoteClusterBlocks(TEST_BLOB_NAME, clusterUUID, compressor);
        assertEquals(remoteObjectForDownload.getBlobFileName(), TEST_BLOB_FILE_NAME);
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/density/discovery-nodes";
        RemoteDiscoveryNodes remoteObjectForDownload = new RemoteDiscoveryNodes(uploadedFile, clusterUUID, compressor);
        assertArrayEquals(remoteObjectForDownload.getBlobPathTokens(), new String[] { "user", "local", "density", "discovery-nodes" });
    }

    public void testBlobPathParameters() {
        DiscoveryNodes nodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteObjectForUpload = new RemoteDiscoveryNodes(nodes, METADATA_VERSION, clusterUUID, compressor);
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertEquals(params.getPathTokens(), List.of(CLUSTER_STATE_EPHEMERAL_PATH_TOKEN));
        assertEquals(params.getFilePrefix(), DISCOVERY_NODES);
    }

    public void testGenerateBlobFileName() {
        DiscoveryNodes nodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteObjectForUpload = new RemoteDiscoveryNodes(nodes, METADATA_VERSION, clusterUUID, compressor);
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertEquals(nameTokens[0], DISCOVERY_NODES);
        assertEquals(RemoteStoreUtils.invertLong(nameTokens[1]), METADATA_VERSION);
        assertTrue(RemoteStoreUtils.invertLong(nameTokens[2]) <= System.currentTimeMillis());
        assertEquals(nameTokens[3], String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION));
    }

    public void testGetUploadedMetadata() throws IOException {
        DiscoveryNodes nodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteObjectForUpload = new RemoteDiscoveryNodes(nodes, METADATA_VERSION, clusterUUID, compressor);
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);
        remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
        assertEquals(uploadedMetadata.getComponent(), DISCOVERY_NODES);
        assertEquals(uploadedMetadata.getUploadedFilename(), remoteObjectForUpload.getFullBlobName());
    }

    public void testSerDe() throws IOException {
        DiscoveryNodes nodes = getDiscoveryNodes();
        RemoteDiscoveryNodes remoteObjectForUpload = new RemoteDiscoveryNodes(nodes, METADATA_VERSION, clusterUUID, compressor);
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertTrue(inputStream.available() > 0);
            DiscoveryNodes readDiscoveryNodes = remoteObjectForUpload.deserialize(inputStream);
            assertEquals(nodes.getSize(), readDiscoveryNodes.getSize());
            nodes.getNodes().forEach((nodeId, node) -> assertEquals(readDiscoveryNodes.get(nodeId), node));
            assertEquals(nodes.getClusterManagerNodeId(), readDiscoveryNodes.getClusterManagerNodeId());
        }
    }

    public void testExceptionDuringSerialization() throws IOException {
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        RemoteDiscoveryNodes remoteObjectForUpload = new RemoteDiscoveryNodes(nodes, METADATA_VERSION, clusterUUID, compressor);
        doThrow(new IOException("mock-exception")).when(nodes).writeToWithAttribute(any());
        IOException iea = assertThrows(IOException.class, remoteObjectForUpload::serialize);
    }

    public void testExceptionDuringDeserialize() throws IOException {
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        InputStream in = mock(InputStream.class);
        when(in.read(any(byte[].class))).thenThrow(new IOException("mock-exception"));
        String uploadedFile = "user/local/density/discovery-nodes";
        RemoteDiscoveryNodes remoteObjectForDownload = new RemoteDiscoveryNodes(uploadedFile, clusterUUID, compressor);
        IOException ioe = assertThrows(IOException.class, () -> remoteObjectForDownload.deserialize(in));
    }

    public static DiscoveryNodes getDiscoveryNodes() {
        return DiscoveryNodes.builder()
            .add(
                new DiscoveryNode(
                    "name_" + 1,
                    "node_" + 1,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES)),
                    Version.CURRENT
                )
            )
            .add(
                new DiscoveryNode(
                    "name_" + 2,
                    "node_" + 2,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES)),
                    Version.CURRENT
                )
            )
            .add(
                new DiscoveryNode(
                    "name_" + 3,
                    "node_" + 3,
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    new HashSet<>(randomSubsetOf(DiscoveryNodeRole.BUILT_IN_ROLES)),
                    Version.CURRENT
                )
            )
            .localNodeId("name_1")
            .clusterManagerNodeId("name_2")
            .build();
    }
}
