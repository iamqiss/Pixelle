/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotestore;

import org.density.common.settings.Settings;
import org.density.index.SegmentReplicationPressureIT;
import org.density.test.DensityIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

import static org.density.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;

/**
 * This class executes the SegmentReplicationPressureIT suite with remote store integration enabled.
 */
@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationWithRemoteStorePressureIT extends SegmentReplicationPressureIT {

    private static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected Path absolutePath;

    @Override
    protected boolean segmentReplicationWithRemoteEnabled() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .build();
    }

    @Before
    public void setup() {
        absolutePath = randomRepoPath().toAbsolutePath();
        internalCluster().startClusterManagerOnlyNode();
    }

    @After
    public void teardown() {
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
    }
}
