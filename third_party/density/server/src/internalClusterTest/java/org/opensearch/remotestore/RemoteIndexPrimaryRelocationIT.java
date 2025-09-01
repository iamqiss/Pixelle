/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotestore;

import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.indices.recovery.IndexPrimaryRelocationIT;
import org.density.indices.replication.common.ReplicationType;
import org.density.test.DensityIntegTestCase;

import java.nio.file.Path;

import static org.density.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteIndexPrimaryRelocationIT extends IndexPrimaryRelocationIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";

    protected Path absolutePath;

    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .build();
    }

    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
    }

    public void testPrimaryRelocationWhileIndexing() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        super.testPrimaryRelocationWhileIndexing();
    }
}
