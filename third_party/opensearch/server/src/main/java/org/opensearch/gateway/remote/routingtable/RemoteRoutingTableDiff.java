/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote.routingtable;

import org.density.cluster.Diff;
import org.density.cluster.routing.IndexRoutingTable;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.routing.RoutingTableIncrementalDiff;
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

import static org.density.gateway.remote.RemoteClusterStateUtils.DELIMITER;

/**
 * Represents a incremental difference between {@link org.density.cluster.routing.RoutingTable} objects that can be serialized and deserialized.
 * This class is responsible for writing and reading the differences between RoutingTables to and from an input/output stream.
 */
public class RemoteRoutingTableDiff extends AbstractClusterMetadataWriteableBlobEntity<Diff<RoutingTable>> {

    private final RoutingTableIncrementalDiff routingTableIncrementalDiff;

    private long term;
    private long version;

    public static final String ROUTING_TABLE_DIFF = "routing-table-diff";

    public static final String ROUTING_TABLE_DIFF_METADATA_PREFIX = "routingTableDiff--";

    public static final String ROUTING_TABLE_DIFF_FILE = "routing_table_diff";
    private static final String codec = "RemoteRoutingTableDiff";
    public static final String ROUTING_TABLE_DIFF_PATH_TOKEN = "routing-table-diff";

    public static final int VERSION = 1;

    public static final ChecksumWritableBlobStoreFormat<RoutingTableIncrementalDiff> REMOTE_ROUTING_TABLE_DIFF_FORMAT =
        new ChecksumWritableBlobStoreFormat<>(codec, RoutingTableIncrementalDiff::readFrom);

    /**
     * Constructs a new RemoteRoutingTableDiff with the given differences.
     *
     * @param routingTableIncrementalDiff a RoutingTableIncrementalDiff object containing the differences of {@link IndexRoutingTable}.
     * @param clusterUUID the cluster UUID.
     * @param compressor the compressor to be used.
     * @param term the term of the routing table.
     * @param version the version of the routing table.
     */
    public RemoteRoutingTableDiff(
        RoutingTableIncrementalDiff routingTableIncrementalDiff,
        String clusterUUID,
        Compressor compressor,
        long term,
        long version
    ) {
        super(clusterUUID, compressor);
        this.routingTableIncrementalDiff = routingTableIncrementalDiff;
        this.term = term;
        this.version = version;
    }

    /**
     * Constructs a new RemoteIndexRoutingTableDiff with the given blob name, cluster UUID, and compressor.
     *
     * @param blobName the name of the blob.
     * @param clusterUUID the cluster UUID.
     * @param compressor the compressor to be used.
     */
    public RemoteRoutingTableDiff(String blobName, String clusterUUID, Compressor compressor) {
        super(clusterUUID, compressor);
        this.routingTableIncrementalDiff = null;
        this.blobName = blobName;
    }

    /**
     * Gets the map of differences of {@link IndexRoutingTable}.
     *
     * @return a map containing the differences.
     */
    public Diff<RoutingTable> getDiffs() {
        return routingTableIncrementalDiff;
    }

    @Override
    public BlobPathParameters getBlobPathParameters() {
        return new BlobPathParameters(List.of(ROUTING_TABLE_DIFF_PATH_TOKEN), ROUTING_TABLE_DIFF_METADATA_PREFIX);
    }

    @Override
    public String getType() {
        return ROUTING_TABLE_DIFF;
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
        return new ClusterMetadataManifest.UploadedMetadataAttribute(ROUTING_TABLE_DIFF_FILE, blobName);
    }

    @Override
    public InputStream serialize() throws IOException {
        assert routingTableIncrementalDiff != null;
        return REMOTE_ROUTING_TABLE_DIFF_FORMAT.serialize(routingTableIncrementalDiff, generateBlobFileName(), getCompressor())
            .streamInput();
    }

    @Override
    public Diff<RoutingTable> deserialize(InputStream in) throws IOException {
        return REMOTE_ROUTING_TABLE_DIFF_FORMAT.deserialize(blobName, Streams.readFully(in));
    }
}
