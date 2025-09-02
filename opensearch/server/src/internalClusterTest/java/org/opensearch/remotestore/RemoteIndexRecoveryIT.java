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
import org.density.core.common.unit.ByteSizeUnit;
import org.density.core.common.unit.ByteSizeValue;
import org.density.index.IndexModule;
import org.density.index.IndexSettings;
import org.density.indices.recovery.IndexRecoveryIT;
import org.density.indices.recovery.RecoverySettings;
import org.density.indices.replication.common.ReplicationType;
import org.density.test.DensityIntegTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

import static org.density.indices.recovery.RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE_SETTING;
import static org.density.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteIndexRecoveryIT extends IndexRecoveryIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";

    protected Path repositoryPath;

    @Before
    public void setup() {
        repositoryPath = randomRepoPath().toAbsolutePath();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, repositoryPath))
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    @Override
    public void slowDownRecovery(ByteSizeValue shardSize) {
        logger.info("--> shardSize: " + shardSize);
        long chunkSize = Math.max(1, shardSize.getBytes() / 50);
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        // one chunk per sec..
                        .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), chunkSize, ByteSizeUnit.BYTES)
                        // small chunks
                        .put(INDICES_RECOVERY_CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkSize, ByteSizeUnit.BYTES))
                )
                .get()
                .isAcknowledged()
        );
    }

    @After
    public void teardown() {
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
    }

    @Override
    protected Matcher<Long> getMatcherForThrottling(long value) {
        return Matchers.greaterThanOrEqualTo(value);
    }

    @Override
    protected int numDocs() {
        return randomIntBetween(100, 200);
    }

    @Override
    public void testUsesFileBasedRecoveryIfRetentionLeaseMissing() {
        // Retention lease based tests not applicable for remote store;
    }

    @Override
    public void testPeerRecoveryTrimsLocalTranslog() {
        // Peer recovery usecase not valid for remote enabled indices
    }

    @Override
    public void testHistoryRetention() {
        // History retention not applicable for remote store
    }

    @Override
    public void testUsesFileBasedRecoveryIfOperationsBasedRecoveryWouldBeUnreasonable() {
        // History retention not applicable for remote store
    }

    @Override
    public void testUsesFileBasedRecoveryIfRetentionLeaseAheadOfGlobalCheckpoint() {
        // History retention not applicable for remote store
    }

    @Override
    public void testRecoverLocallyUpToGlobalCheckpoint() {
        // History retention not applicable for remote store
    }

    @Override
    public void testCancelNewShardRecoveryAndUsesExistingShardCopy() {
        // History retention not applicable for remote store
    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testReservesBytesDuringPeerRecoveryPhaseOne() {

    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testAllocateEmptyPrimaryResetsGlobalCheckpoint() {

    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testDoesNotCopyOperationsInSafeCommit() {

    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testRepeatedRecovery() {

    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testDisconnectsWhileRecovering() {

    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testTransientErrorsDuringRecoveryAreRetried() {

    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testDoNotInfinitelyWaitForMapping() {

    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testDisconnectsDuringRecovery() {

    }

    @AwaitsFix(bugUrl = "https://github.com/density-project/Density/issues/8919")
    @Override
    public void testReplicaRecovery() {

    }
}
