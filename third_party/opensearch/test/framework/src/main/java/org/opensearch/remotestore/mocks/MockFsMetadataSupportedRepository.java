/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotestore.mocks;

import org.density.cluster.metadata.RepositoryMetadata;
import org.density.cluster.service.ClusterService;
import org.density.common.blobstore.BlobStore;
import org.density.common.blobstore.fs.FsBlobStore;
import org.density.common.settings.Setting;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.indices.recovery.RecoverySettings;
import org.density.repositories.fs.ReloadableFsRepository;

public class MockFsMetadataSupportedRepository extends ReloadableFsRepository {

    public static Setting<Boolean> TRIGGER_DATA_INTEGRITY_FAILURE = Setting.boolSetting(
        "mock_fs_repository.trigger_data_integrity_failure",
        false
    );

    private final boolean triggerDataIntegrityFailure;

    public MockFsMetadataSupportedRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        super(metadata, environment, namedXContentRegistry, clusterService, recoverySettings);
        triggerDataIntegrityFailure = TRIGGER_DATA_INTEGRITY_FAILURE.get(metadata.settings());
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        FsBlobStore fsBlobStore = (FsBlobStore) super.createBlobStore();
        return new MockFsMetadataSupportedBlobStore(
            fsBlobStore.bufferSizeInBytes(),
            fsBlobStore.path(),
            isReadOnly(),
            triggerDataIntegrityFailure
        );
    }
}
