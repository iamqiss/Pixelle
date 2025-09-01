/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.model;

import org.density.cluster.metadata.Metadata;
import org.density.common.io.Streams;
import org.density.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.density.common.remote.BlobPathParameters;
import org.density.core.compress.Compressor;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.gateway.remote.ClusterMetadataManifest.UploadedMetadata;
import org.density.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;

import static org.density.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;

/**
 * Wrapper class for uploading/downloading global metadata ({@link Metadata}) to/from remote blob store
 */
public class RemoteGlobalMetadata extends AbstractClusterMetadataWriteableBlobEntity<Metadata> {
    public static final String GLOBAL_METADATA = "global_metadata";

    public static final ChecksumBlobStoreFormat<Metadata> GLOBAL_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "metadata",
        METADATA_NAME_FORMAT,
        Metadata::fromXContent
    );

    public RemoteGlobalMetadata(
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
        throw new UnsupportedOperationException();
    }

    @Override
    public String getType() {
        return GLOBAL_METADATA;
    }

    @Override
    public String generateBlobFileName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public UploadedMetadata getUploadedMetadata() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream serialize() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Metadata deserialize(final InputStream inputStream) throws IOException {
        return GLOBAL_METADATA_FORMAT.deserialize(blobName, getNamedXContentRegistry(), Streams.readFully(inputStream));
    }
}
