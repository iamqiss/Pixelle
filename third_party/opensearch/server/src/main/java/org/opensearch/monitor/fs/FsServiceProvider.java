/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.monitor.fs;

import org.density.cluster.node.DiscoveryNode;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.env.NodeEnvironment;
import org.density.index.store.remote.filecache.FileCache;
import org.density.index.store.remote.filecache.FileCacheSettings;
import org.density.indices.IndicesService;

/**
 * Factory for creating appropriate FsService implementations based on node type.
 *
 * @density.internal
 */
public class FsServiceProvider {

    private final Settings settings;
    private final NodeEnvironment nodeEnvironment;
    private final FileCache fileCache;
    private final FileCacheSettings fileCacheSettings;
    private final IndicesService indicesService;

    public FsServiceProvider(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        FileCache fileCache,
        ClusterSettings clusterSettings,
        IndicesService indicesService
    ) {
        this.settings = settings;
        this.nodeEnvironment = nodeEnvironment;
        this.fileCache = fileCache;
        this.fileCacheSettings = new FileCacheSettings(settings, clusterSettings);
        this.indicesService = indicesService;
    }

    /**
     * Creates the appropriate FsService implementation based on node type.
     *
     * @return FsService instance
     */
    public FsService createFsService() {
        if (DiscoveryNode.isWarmNode(settings)) {
            return new WarmFsService(settings, nodeEnvironment, fileCacheSettings, indicesService, fileCache);
        }
        return new FsService(settings, nodeEnvironment, fileCache);
    }
}
