/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotestore.multipart.mocks;

import org.density.cluster.service.ClusterService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.indices.recovery.RecoverySettings;
import org.density.plugins.Plugin;
import org.density.plugins.RepositoryPlugin;
import org.density.repositories.Repository;

import java.util.Collections;
import java.util.Map;

public class MockFsRepositoryPlugin extends Plugin implements RepositoryPlugin {

    public static final String TYPE = "fs_multipart_repository";

    @Override
    public Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            "fs_multipart_repository",
            metadata -> new MockFsRepository(metadata, env, namedXContentRegistry, clusterService, recoverySettings)
        );
    }
}
