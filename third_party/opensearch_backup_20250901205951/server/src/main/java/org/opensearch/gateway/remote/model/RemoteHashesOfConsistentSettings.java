/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.cluster.metadata.DiffableStringMap;
import org.density.common.io.Streams;
import org.density.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.density.common.remote.BlobPathParameters;
import org.density.core.compress.Compressor;
import org.density.gateway.remote.ClusterMetadataManifest;
import org.density.index.remote.RemoteStoreUtils;
import org.density.repositories.blobstore.ChecksumWritableBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.density.gateway.remote.RemoteClusterStateAttributesManager.CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION;
import static org.density.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.density.gateway.remote.RemoteClusterStateUtils.GLOBAL_METADATA_PATH_TOKEN;

/**
 * Wrapper class for uploading/downloading {@link DiffableStringMap} to/from remote blob store
 */
public class RemoteHashesOfConsistentSettings extends AbstractClusterMetadataWriteableBlobEntity<DiffableStringMap> {
    public static final String HASHES_OF_CONSISTENT_SETTINGS = "hashes-of-consistent-settings";
    public static final ChecksumWritableBlobStoreFormat<DiffableStringMap> HASHES_OF_CONSISTENT_SETTINGS_FORMAT =
        new ChecksumWritableBlobStoreFormat<>("hashes-of-consistent-settings", DiffableStringMap::readFrom);

    private DiffableStringMap hashesOfConsistentSettings;
    private long metadataVersion;

    public RemoteHashesOfConsistentSettings(
        final DiffableStringMap hashesOfConsistentSettings,
        final long metadataVersion,
        final String clusterUUID,
        final Compressor compressor
    ) {
        super(clusterUUID, compressor, null);
        this.metadataVersion = metadataVersion;
        this.hashesOfConsistentSettings = hashesOfConsistentSettings;
    }

    public RemoteHashesOfConsistentSettings(final String blobName, final String clusterUUID, final Compressor compressor) {
        super(clusterUUID, compressor, null);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(GLOBAL_METADATA_PATH_TOKEN), HASHES_OF_CONSISTENT_SETTINGS);
    }

    @Override
    public String getType() {
        return HASHES_OF_CONSISTENT_SETTINGS;
    }

    @Override
    public String generateBlobFileName() {
        String blobFileName = String.join(
            DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION)
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public ClusterMetadataManifest.UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new ClusterMetadataManifest.UploadedMetadataAttribute(HASHES_OF_CONSISTENT_SETTINGS, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return HASHES_OF_CONSISTENT_SETTINGS_FORMAT.serialize(hashesOfConsistentSettings, generateBlobFileName(), getCompressor())
            .streamInput();
    }

    @Override
    public DiffableStringMap deserialize(final InputStream inputStream) throws IOException {
        return HASHES_OF_CONSISTENT_SETTINGS_FORMAT.deserialize(blobName, Streams.readFully(inputStream));
    }
}
