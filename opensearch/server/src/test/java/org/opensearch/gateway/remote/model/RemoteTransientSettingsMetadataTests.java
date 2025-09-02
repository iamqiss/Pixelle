/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.cluster.ClusterModule;
import org.density.common.blobstore.BlobPath;
import org.density.common.network.NetworkModule;
import org.density.common.remote.BlobPathParameters;
import org.density.common.settings.Settings;
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
import static org.density.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_CURRENT_CODEC_VERSION;
import static org.density.gateway.remote.model.RemoteTransientSettingsMetadata.TRANSIENT_SETTING_METADATA;

public class RemoteTransientSettingsMetadataTests extends DensityTestCase {
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
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(remoteObjectForUpload.clusterUUID(), clusterUUID);

        RemoteTransientSettingsMetadata remoteObjectForDownload = new RemoteTransientSettingsMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(remoteObjectForDownload.clusterUUID(), clusterUUID);
    }

    public void testFullBlobName() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertNull(remoteObjectForUpload.getFullBlobName());

        RemoteTransientSettingsMetadata remoteObjectForDownload = new RemoteTransientSettingsMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(remoteObjectForDownload.getFullBlobName(), TEST_BLOB_NAME);
    }

    public void testBlobFileName() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertNull(remoteObjectForUpload.getBlobFileName());

        RemoteTransientSettingsMetadata remoteObjectForDownload = new RemoteTransientSettingsMetadata(
            TEST_BLOB_NAME,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertEquals(remoteObjectForDownload.getBlobFileName(), TEST_BLOB_FILE_NAME);
    }

    public void testBlobPathTokens() {
        String uploadedFile = "user/local/density/settings";
        RemoteTransientSettingsMetadata remoteObjectForDownload = new RemoteTransientSettingsMetadata(
            uploadedFile,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertArrayEquals(remoteObjectForDownload.getBlobPathTokens(), new String[] { "user", "local", "density", "settings" });
    }

    public void testBlobPathParameters() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        BlobPathParameters params = remoteObjectForUpload.getBlobPathParameters();
        assertEquals(params.getPathTokens(), List.of(RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN));
        assertEquals(params.getFilePrefix(), TRANSIENT_SETTING_METADATA);
    }

    public void testGenerateBlobFileName() {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        String blobFileName = remoteObjectForUpload.generateBlobFileName();
        String[] nameTokens = blobFileName.split(RemoteClusterStateUtils.DELIMITER);
        assertEquals(nameTokens[0], TRANSIENT_SETTING_METADATA);
        assertEquals(RemoteStoreUtils.invertLong(nameTokens[1]), METADATA_VERSION);
        assertTrue(RemoteStoreUtils.invertLong(nameTokens[2]) <= System.currentTimeMillis());
        assertEquals(nameTokens[3], String.valueOf(GLOBAL_METADATA_CURRENT_CODEC_VERSION));

    }

    public void testGetUploadedMetadata() throws IOException {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        assertThrows(AssertionError.class, remoteObjectForUpload::getUploadedMetadata);

        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(new BlobPath().add(TEST_BLOB_PATH));
            ClusterMetadataManifest.UploadedMetadata uploadedMetadata = remoteObjectForUpload.getUploadedMetadata();
            assertEquals(uploadedMetadata.getComponent(), TRANSIENT_SETTING_METADATA);
            assertEquals(uploadedMetadata.getUploadedFilename(), remoteObjectForUpload.getFullBlobName());
        }
    }

    public void testSerDe() throws IOException {
        Settings settings = getSettings();
        RemoteTransientSettingsMetadata remoteObjectForUpload = new RemoteTransientSettingsMetadata(
            settings,
            METADATA_VERSION,
            clusterUUID,
            compressor,
            namedXContentRegistry
        );
        try (InputStream inputStream = remoteObjectForUpload.serialize()) {
            remoteObjectForUpload.setFullBlobName(BlobPath.cleanPath());
            assertTrue(inputStream.available() > 0);
            Settings readsettings = remoteObjectForUpload.deserialize(inputStream);
            assertEquals(readsettings, settings);
        }
    }

    private Settings getSettings() {
        return Settings.builder().put("random_index_setting_" + randomAlphaOfLength(3), randomAlphaOfLength(5)).build();
    }
}
