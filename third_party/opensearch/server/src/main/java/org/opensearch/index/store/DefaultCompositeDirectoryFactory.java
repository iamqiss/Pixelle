/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.density.index.IndexSettings;
import org.density.index.shard.ShardPath;
import org.density.index.store.remote.filecache.FileCache;
import org.density.plugins.IndexStorePlugin;
import org.density.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Default composite directory factory
 */
public class DefaultCompositeDirectoryFactory implements IndexStorePlugin.CompositeDirectoryFactory {

    private static final Logger logger = LogManager.getLogger(DefaultCompositeDirectoryFactory.class);

    @Override
    public Directory newDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath,
        IndexStorePlugin.DirectoryFactory localDirectoryFactory,
        Directory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool
    ) throws IOException {
        logger.trace("Creating composite directory from core - Default CompositeDirectoryFactory");
        Directory localDirectory = localDirectoryFactory.newDirectory(indexSettings, shardPath);
        return new CompositeDirectory(localDirectory, remoteDirectory, fileCache, threadPool);
    }
}
