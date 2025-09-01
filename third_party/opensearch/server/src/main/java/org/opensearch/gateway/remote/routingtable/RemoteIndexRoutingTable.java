/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.routingtable;

import org.density.cluster.routing.IndexRoutingTable;
import org.density.common.io.Streams;
import org.density.common.remote.AbstractClusterMetadataWriteableBlobEntity;
import org.density.common.remote.BlobPathParameters;
import org.density.core.compress.Compressor;
import org.density.core.index.Index;
import org.density.gateway.remote.ClusterMetadataManifest;
import org.density.index.remote.RemoteStoreUtils;
import org.density.repositories.blobstore.ChecksumWritableBlobStoreFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.density.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * Remote store object for IndexRoutingTable
 */
public class RemoteIndexRoutingTable extends AbstractClusterMetadataWriteableBlobEntity<IndexRoutingTable> {

    public static final String INDEX_ROUTING_TABLE = "index-routing";
    public static final String INDEX_ROUTING_METADATA_PREFIX = "indexRouting--";
    public static final String INDEX_ROUTING_FILE = "index_routing";
    private IndexRoutingTable indexRoutingTable;
    private final Index index;
    private long term;
    private long version;
    private BlobPathParameters blobPathParameters;
    public static final ChecksumWritableBlobStoreFormat<IndexRoutingTable> INDEX_ROUTING_TABLE_FORMAT =
        new ChecksumWritableBlobStoreFormat<>("index-routing-table", IndexRoutingTable::readFrom);

    public RemoteIndexRoutingTable(
        IndexRoutingTable indexRoutingTable,
        String clusterUUID,
        Compressor compressor,
        long term,
        long version
    ) {
        super(clusterUUID, compressor);
        this.index = indexRoutingTable.getIndex();
        this.indexRoutingTable = indexRoutingTable;
        this.term = term;
        this.version = version;
    }

    /**
     * Reads data from inputStream and creates RemoteIndexRoutingTable object with the {@link IndexRoutingTable}
     * @param blobName name of the blob, which contains the index routing data
     * @param clusterUUID UUID of the cluster
     * @param compressor Compressor object
     */
    public RemoteIndexRoutingTable(String blobName, String clusterUUID, Compressor compressor) {
        super(clusterUUID, compressor);
        this.index = null;
        this.term = -1;
        this.version = -1;
        this.blobName = blobName;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        if (blobPathParameters == null) {
            blobPathParameters = new BlobPathParameters(List.of(indexRoutingTable.getIndex().getUUID()), INDEX_ROUTING_FILE);
        }
        return blobPathParameters;
    }

    @Override
    public String getType() {
        return INDEX_ROUTING_TABLE;
    }

    @Override
    public String generateBlobFileName() {
        if (blobFileName == null) {
            blobFileName = String.join(
                DELIMITER,
                getBlobPathParameters().getFilePrefix(),
                RemoteStoreUtils.invertLong(term),
                RemoteStoreUtils.invertLong(version),
                RemoteStoreUtils.invertLong(System.currentTimeMillis())
            );
        }
        return blobFileName;
    }

    @Override
    public ClusterMetadataManifest.UploadedMetadata getUploadedMetadata() {
        assert blobName != null;
        assert index != null;
        return new ClusterMetadataManifest.UploadedIndexMetadata(index.getName(), index.getUUID(), blobName, INDEX_ROUTING_METADATA_PREFIX);
    }

    @Override
    public InputStream serialize() throws IOException {
        return INDEX_ROUTING_TABLE_FORMAT.serialize(indexRoutingTable, generateBlobFileName(), getCompressor()).streamInput();
    }

    @Override
    public IndexRoutingTable deserialize(InputStream in) throws IOException {
        return INDEX_ROUTING_TABLE_FORMAT.deserialize(blobName, Streams.readFully(in));
    }
}
