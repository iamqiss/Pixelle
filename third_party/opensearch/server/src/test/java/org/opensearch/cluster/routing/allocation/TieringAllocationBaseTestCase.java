/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.routing.allocation;

import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.common.SuppressForbidden;
import org.density.common.settings.Settings;
import org.density.index.IndexModule;

import static org.density.index.IndexModule.INDEX_TIERING_STATE;

@SuppressForbidden(reason = "feature flag overrides")
public abstract class TieringAllocationBaseTestCase extends RemoteShardsBalancerBaseTestCase {

    public ClusterState updateIndexMetadataForTiering(
        ClusterState clusterState,
        int localIndices,
        int remoteIndices,
        String tieringState,
        boolean isWarmIndex
    ) {
        Metadata.Builder mb = Metadata.builder(clusterState.metadata());
        for (int i = 0; i < localIndices; i++) {
            IndexMetadata indexMetadata = clusterState.metadata().index(getIndexName(i, false));
            Settings settings = indexMetadata.getSettings();
            mb.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(
                        Settings.builder()
                            .put(settings)
                            .put(settings)
                            .put(INDEX_TIERING_STATE.getKey(), tieringState)
                            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), isWarmIndex)
                    )
            );
        }
        for (int i = 0; i < remoteIndices; i++) {
            IndexMetadata indexMetadata = clusterState.metadata().index(getIndexName(i, true));
            Settings settings = indexMetadata.getSettings();
            mb.put(
                IndexMetadata.builder(indexMetadata)
                    .settings(
                        Settings.builder()
                            .put(settings)
                            .put(settings)
                            .put(INDEX_TIERING_STATE.getKey(), tieringState)
                            .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), isWarmIndex)
                    )
            );
        }
        Metadata metadata = mb.build();
        return ClusterState.builder(clusterState).metadata(metadata).build();
    }
}
