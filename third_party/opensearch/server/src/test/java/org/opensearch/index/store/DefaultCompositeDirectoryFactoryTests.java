/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.density.common.settings.Settings;
import org.density.core.common.breaker.CircuitBreaker;
import org.density.core.common.breaker.NoopCircuitBreaker;
import org.density.core.index.shard.ShardId;
import org.density.index.IndexSettings;
import org.density.index.shard.ShardPath;
import org.density.index.store.remote.filecache.FileCache;
import org.density.index.store.remote.filecache.FileCacheFactory;
import org.density.plugins.IndexStorePlugin;
import org.density.test.IndexSettingsModule;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultCompositeDirectoryFactoryTests extends BaseRemoteSegmentStoreDirectoryTests {

    private DefaultCompositeDirectoryFactory directoryFactory;
    private IndexSettings indexSettings;
    private ShardPath shardPath;
    private IndexStorePlugin.DirectoryFactory localDirectoryFactory;
    private FSDirectory localDirectory;
    private FileCache fileCache;

    @Before
    public void setup() throws IOException {
        indexSettings = IndexSettingsModule.newIndexSettings("foo", Settings.builder().build());
        Path tempDir = createTempDir().resolve(indexSettings.getUUID()).resolve("0");
        shardPath = new ShardPath(false, tempDir, tempDir, new ShardId(indexSettings.getIndex(), 0));
        localDirectoryFactory = mock(IndexStorePlugin.DirectoryFactory.class);
        localDirectory = FSDirectory.open(createTempDir());
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(10000, new NoopCircuitBreaker(CircuitBreaker.REQUEST));
        when(localDirectoryFactory.newDirectory(indexSettings, shardPath)).thenReturn(localDirectory);
        setupRemoteSegmentStoreDirectory();
        populateMetadata();
        remoteSegmentStoreDirectory.init();
    }

    public void testNewDirectory() throws IOException {
        directoryFactory = new DefaultCompositeDirectoryFactory();
        Directory directory = directoryFactory.newDirectory(
            indexSettings,
            shardPath,
            localDirectoryFactory,
            remoteSegmentStoreDirectory,
            fileCache,
            threadPool
        );
        assertNotNull(directory);
        assert (directory instanceof CompositeDirectory);
        verify(localDirectoryFactory).newDirectory(indexSettings, shardPath);
    }

}
