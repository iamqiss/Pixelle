/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.routing.remote;

import org.density.action.LatchedActionListener;
import org.density.cluster.Diff;
import org.density.cluster.routing.IndexRoutingTable;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.routing.StringKeyDiffProvider;
import org.density.common.lifecycle.LifecycleComponent;
import org.density.gateway.remote.ClusterMetadataManifest;

import java.io.IOException;
import java.util.List;

/**
 * A Service which provides APIs to upload and download routing table from remote store.
 *
 * @density.internal
 */
public interface RemoteRoutingTableService extends LifecycleComponent {

    List<IndexRoutingTable> getIndicesRouting(RoutingTable routingTable);

    void getAsyncIndexRoutingReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<IndexRoutingTable> latchedActionListener
    );

    void getAsyncIndexRoutingTableDiffReadAction(
        String clusterUUID,
        String uploadedFilename,
        LatchedActionListener<Diff<RoutingTable>> latchedActionListener
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getUpdatedIndexRoutingTableMetadata(
        List<String> updatedIndicesRouting,
        List<ClusterMetadataManifest.UploadedIndexMetadata> allIndicesRouting
    );

    StringKeyDiffProvider<IndexRoutingTable> getIndicesRoutingMapDiff(RoutingTable before, RoutingTable after);

    void getAsyncIndexRoutingWriteAction(
        String clusterUUID,
        long term,
        long version,
        IndexRoutingTable indexRouting,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    );

    void getAsyncIndexRoutingDiffWriteAction(
        String clusterUUID,
        long term,
        long version,
        StringKeyDiffProvider<IndexRoutingTable> routingTableDiff,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    );

    List<ClusterMetadataManifest.UploadedIndexMetadata> getAllUploadedIndicesRouting(
        ClusterMetadataManifest previousManifest,
        List<ClusterMetadataManifest.UploadedIndexMetadata> indicesRoutingUploaded,
        List<String> indicesRoutingToDelete
    );

    void deleteStaleIndexRoutingPaths(List<String> stalePaths) throws IOException;

    void deleteStaleIndexRoutingDiffPaths(List<String> stalePaths) throws IOException;

}
