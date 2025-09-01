/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.repositories.blobstore;

import org.density.cluster.metadata.RepositoryMetadata;
import org.density.cluster.service.ClusterService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.indices.recovery.RecoverySettings;
import org.density.repositories.RepositoryInfo;
import org.density.repositories.RepositoryStatsSnapshot;

import java.util.Map;

/**
 * A blob store repository that is metered
 *
 * @density.internal
 */
public abstract class MeteredBlobStoreRepository extends BlobStoreRepository {
    private final RepositoryInfo repositoryInfo;

    public MeteredBlobStoreRepository(
        RepositoryMetadata metadata,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings,
        Map<String, String> location
    ) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings);
        this.repositoryInfo = new RepositoryInfo(metadata.name(), metadata.type(), location);
    }

    @Override
    public void reload(RepositoryMetadata repositoryMetadata) {
        super.reload(repositoryMetadata);

        // Not adding any additional reload logic here is intentional as the constructor only
        // initializes the repositoryInfo from the repo metadata, which cannot be changed.
    }

    public RepositoryStatsSnapshot statsSnapshot() {
        return new RepositoryStatsSnapshot(repositoryInfo, stats(), RepositoryStatsSnapshot.UNKNOWN_CLUSTER_VERSION);
    }
}
