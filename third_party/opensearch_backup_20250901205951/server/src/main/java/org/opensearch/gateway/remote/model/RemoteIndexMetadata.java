/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.cluster.metadata.IndexMetadata;
import org.density.common.blobstore.BlobPath;
import org.density.common.io.Streams;
import org.density.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.density.common.remote.BlobPathParameters;
import org.density.core.compress.Compressor;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.gateway.remote.ClusterMetadataManifest.UploadedIndexMetadata;
import org.density.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.density.gateway.remote.RemoteClusterStateUtils;
import org.density.index.remote.RemoteStoreEnums;
import org.density.index.remote.RemoteStorePathStrategy;
import org.density.index.remote.RemoteStoreUtils;
import org.density.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.density.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_PLAIN_FORMAT;

/**
 * Wrapper class for uploading/downloading {@link IndexMetadata} to/from remote blob store
 */
public class RemoteIndexMetadata extends AbstractClusterMetadataWriteableBlobEntity<IndexMetadata> {

    public static final int INDEX_METADATA_CURRENT_CODEC_VERSION = 2;

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_PLAIN_FORMAT,
        IndexMetadata::fromXContent
    );
    public static final String INDEX = "index";

    private IndexMetadata indexMetadata;
    private RemoteStoreEnums.PathType pathType;
    private RemoteStoreEnums.PathHashAlgorithm pathHashAlgo;
    private String fixedPrefix;

    public RemoteIndexMetadata(
        final IndexMetadata indexMetadata,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry,
        final RemoteStoreEnums.PathType pathType,
        final RemoteStoreEnums.PathHashAlgorithm pathHashAlgo,
        final String fixedPrefix
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.indexMetadata = indexMetadata;
        this.pathType = pathType;
        this.pathHashAlgo = pathHashAlgo;
        this.fixedPrefix = fixedPrefix;
    }

    public RemoteIndexMetadata(
        final String blobName,
        final String clusterUUID,
        final Compressor compressor,
        final NamedXContentRegistry namedXContentRegistry
    ) {
        super(clusterUUID, compressor, namedXContentRegistry);
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(INDEX, indexMetadata.getIndexUUID()), "metadata");
    }

    @Override
    public String getType() {
        return INDEX;
    }

    @Override
    public String generateBlobFileName() {
        String blobFileName = String.join(
            RemoteClusterStateUtils.DELIMITER,
            getBlobPathParameters().getFilePrefix(),
            RemoteStoreUtils.invertLong(indexMetadata.getVersion()),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION) // Keep the codec version at last place only, during reads we read last
            // place to determine codec version.
        );
        this.blobFileName = blobFileName;
        return blobFileName;
    }

    @Override
    public BlobPath getPrefixedPath(BlobPath blobPath) {
        if (pathType == null) {
            return blobPath;
        }
        assert pathHashAlgo != null;
        return pathType.path(
            RemoteStorePathStrategy.PathInput.builder()
                .fixedPrefix(fixedPrefix)
                .basePath(blobPath)
                .indexUUID(indexMetadata.getIndexUUID())
                .build(),
            pathHashAlgo
        );
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        return new UploadedIndexMetadata(indexMetadata.getIndex().getName(), indexMetadata.getIndexUUID(), blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        return INDEX_METADATA_FORMAT.serialize(
            indexMetadata,
            generateBlobFileName(),
            getCompressor(),
            RemoteClusterStateUtils.FORMAT_PARAMS
        ).streamInput();
    }

    @Override
    public IndexMetadata deserialize(final InputStream inputStream) throws IOException {
        // Blob name parameter is redundant
        return INDEX_METADATA_FORMAT.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }

}
