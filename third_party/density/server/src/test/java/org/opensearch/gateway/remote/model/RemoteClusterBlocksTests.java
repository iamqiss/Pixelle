/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.cluster.ClusterModule;
import org.density.cluster.block.ClusterBlocks;
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
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.density.cluster.block.ClusterBlockTests.randomClusterBlock;
import static org.density.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.density.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_EPHEMERAL_PATH_TOKEN;
import static org.density.gateway.remote.model.RemoteClusterBlocks.CLUSTER_BLOCKS;

public class RemoteClusterBlocksTests extends DensityTestCase {
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
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, METADATA_VERSION, clusterUUID, compressor);
        assertEquals(remoteObjectForUpload.clusterUUID(), clusterUUID);

        RemoteClusterBlocks remoteObjectForDownload = new RemoteClusterBlocks(TEST_BLOB_NAME, clusterUUID, compressor);
        assertEquals(remoteObjectForDownload.clusterUUID(), clusterUUID);
    }

    public void testFullBlobName() {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, METADATA_VERSION, clusterUUID, compressor);
        assertNull(remoteObjectForUpload.getFullBlobName());

        RemoteClusterBlocks remoteObjectForDownload = new RemoteClusterBlocks(TEST_BLOB_NAME, clusterUUID, compressor);
        assertEquals(remoteObjectForDownload.getFullBlobName(), TEST_BLOB_NAME);
    }

    public void testBlobFileName() {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, METADATA_VERSION, clusterUUID, compressor);
        assertNull(remoteObjectForUpload.getBlobFileName());

        RemoteClusterBlocks remoteObjectForDownload = new RemoteClusterBlocks(TEST_BLOB_NAME, clusterUUID, compressor);
        assertEquals(remoteObjectForDownload.getBlobFileName(), TEST_BLOB_FILE_NAME);
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/density/cluster-blocks";
        RemoteClusterBlocks remoteObjectForDownload = new RemoteClusterBlocks(uploadedFile, clusterUUID, compressor);
        assertArrayEquals(remoteObjectForDownload.getBlobPathTokens(), new String[] { "user", "local", "density", "cluster-blocks" });
    }

    public void testBlobPathParameters() {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, METADATA_VERSION, clusterUUID, compressor);
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertEquals(params.getPathTokens(), List.of(CLUSTER_STATE_EPHEMERAL_PATH_TOKEN));
        assertEquals(params.getFilePrefix(), CLUSTER_BLOCKS);
    }

    public void testGenerateBlobFileName() {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, METADATA_VERSION, clusterUUID, compressor);
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertEquals(nameTokens[0], CLUSTER_BLOCKS);
        assertEquals(RemoteStoreUtils.invertLong(nameTokens[1]), METADATA_VERSION);
        assertTrue(RemoteStoreUtils.invertLong(nameTokens[2]) <= System.currentTimeMillis());
        assertEquals(nameTokens[3], String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION));

    }

    public void testGetUploadedMetadata() throws IOException {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, METADATA_VERSION, clusterUUID, compressor);
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);
        remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
        ClusterMetadataManifest.UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
        assertEquals(uploadedMetadata.getComponent(), CLUSTER_BLOCKS);
        assertEquals(uploadedMetadata.getUploadedFilename(), remoteObjectForUpload.getFullBlobName());
    }

    public void testSerDe() throws IOException {
        ClusterBlocks clusterBlocks = randomClusterBlocks();
        RemoteClusterBlocks remoteObjectForUpload = new RemoteClusterBlocks(clusterBlocks, METADATA_VERSION, clusterUUID, compressor);
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertTrue(inputStream.available() > 0);
            ClusterBlocks readClusterBlocks = remoteObjectForUpload.deserialize(inputStream);
            assertEquals(clusterBlocks.global(), readClusterBlocks.global());
            assertEquals(clusterBlocks.indices().keySet(), readClusterBlocks.indices().keySet());
            for (String index : clusterBlocks.indices().keySet()) {
                assertEquals(clusterBlocks.indices().get(index), readClusterBlocks.indices().get(index));
            }

        }
    }

    public static ClusterBlocks randomClusterBlocks() {
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        int randomGlobalBlocks = randomIntBetween(1, 10);
        for (int i = 0; i < randomGlobalBlocks; i++) {
            builder.addGlobalBlock(randomClusterBlock());
        }

        int randomIndices = randomIntBetween(1, 10);
        for (int i = 0; i < randomIndices; i++) {
            int randomIndexBlocks = randomIntBetween(1, 10);
            for (int j = 0; j < randomIndexBlocks; j++) {
                builder.addIndexBlock("index-" + i, randomClusterBlock());
            }
        }
        return builder.build();
    }
}
