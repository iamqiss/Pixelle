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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.repositories.fs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.cluster.metadata.RepositoryMetadata;
import org.density.cluster.service.ClusterService;
import org.density.common.blobstore.BlobPath;
import org.density.common.blobstore.BlobStore;
import org.density.common.blobstore.fs.FsBlobStore;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.core.common.Strings;
import org.density.core.common.unit.ByteSizeValue;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.indices.recovery.RecoverySettings;
import org.density.repositories.RepositoryException;
import org.density.repositories.blobstore.BlobStoreRepository;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code location}</dt><dd>Path to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt><dd>Large file can be divided into chunks. This parameter specifies the chunk size.
 *      Defaults to not chucked.</dd>
 * </dl>
 *
 * @density.internal
 */
public class FsRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(FsRepository.class);

    public static final String TYPE = "fs";

    public static final Setting<String> LOCATION_SETTING = new Setting<>("location", "", Function.identity(), Property.NodeScope);
    public static final Setting<String> REPOSITORIES_LOCATION_SETTING = new Setting<>(
        "repositories.fs.location",
        LOCATION_SETTING,
        Function.identity(),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "chunk_size",
        new ByteSizeValue(Long.MAX_VALUE),
        new ByteSizeValue(5),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> REPOSITORIES_CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "repositories.fs.chunk_size",
        new ByteSizeValue(Long.MAX_VALUE),
        new ByteSizeValue(5),
        new ByteSizeValue(Long.MAX_VALUE),
        Property.NodeScope
    );
    public static final Setting<Boolean> REPOSITORIES_COMPRESS_SETTING = Setting.boolSetting(
        "repositories.fs.compress",
        false,
        Property.NodeScope,
        Property.Deprecated
    );

    public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

    protected final Environment environment;

    protected ByteSizeValue chunkSize;

    protected BlobPath basePath;

    /**
     * Constructs a shared file system repository.
     */
    public FsRepository(
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings);
        this.environment = environment;
        validateLocation();
        readMetadata();
    }

    protected void readMetadata() {
        if (CHUNK_SIZE_SETTING.exists(metadata.settings())) {
            this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());
        } else {
            this.chunkSize = REPOSITORIES_CHUNK_SIZE_SETTING.get(environment.settings());
        }
        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            this.basePath = new BlobPath().add(basePath);
        } else {
            this.basePath = BlobPath.cleanPath();
        }
    }

    protected void validateLocation() {
        String location = REPOSITORIES_LOCATION_SETTING.get(metadata.settings());
        if (location.isEmpty()) {
            logger.warn(
                "the repository location is missing, it should point to a shared file system location"
                    + " that is available on all cluster-manager and data nodes"
            );
            throw new RepositoryException(metadata.name(), "missing location");
        }
        Path locationFile = environment.resolveRepoFile(location);
        if (locationFile == null) {
            if (environment.repoFiles().length > 0) {
                logger.warn(
                    "The specified location [{}] doesn't start with any " + "repository paths specified by the path.repo setting: [{}] ",
                    location,
                    environment.repoFiles()
                );
                throw new RepositoryException(
                    metadata.name(),
                    "location [" + location + "] doesn't match any of the locations specified by path.repo"
                );
            } else {
                logger.warn(
                    "The specified location [{}] should start with a repository path specified by"
                        + " the path.repo setting, but the path.repo setting was not set on this node",
                    location
                );
                throw new RepositoryException(
                    metadata.name(),
                    "location [" + location + "] doesn't match any of the locations specified by path.repo because this setting is empty"
                );
            }
        }
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        final String location = REPOSITORIES_LOCATION_SETTING.get(getMetadata().settings());
        final Path locationFile = environment.resolveRepoFile(location);
        return new FsBlobStore(bufferSize, locationFile, isReadOnly());
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    public List<Setting<?>> getRestrictedSystemRepositorySettings() {
        List<Setting<?>> restrictedSettings = new ArrayList<>();
        restrictedSettings.addAll(super.getRestrictedSystemRepositorySettings());
        restrictedSettings.add(LOCATION_SETTING);
        return restrictedSettings;
    }
}
