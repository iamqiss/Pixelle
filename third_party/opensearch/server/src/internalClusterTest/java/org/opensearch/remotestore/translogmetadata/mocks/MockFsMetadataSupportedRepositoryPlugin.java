/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotestore.translogmetadata.mocks;

import org.density.cluster.service.ClusterService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.indices.recovery.RecoverySettings;
import org.density.plugins.Plugin;
import org.density.plugins.RepositoryPlugin;
import org.density.repositories.Repository;

import java.util.Collections;
import java.util.Map;

public class MockFsMetadataSupportedRepositoryPlugin extends Plugin implements RepositoryPlugin {

    public static final String TYPE_MD = "fs_metadata_supported_repository";

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            "fs_metadata_supported_repository",
            metadata -> new MockFsMetadataSupportedRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
        );
    }
}
