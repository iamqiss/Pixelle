/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.cache.clear;

import org.density.action.support.ActionFilters;
import org.density.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlock;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.block.ClusterBlocks;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.core.common.breaker.NoopCircuitBreaker;
import org.density.core.index.shard.ShardId;
import org.density.core.rest.RestStatus;
import org.density.env.Environment;
import org.density.env.NodeEnvironment;
import org.density.env.TestEnvironment;
import org.density.index.shard.ShardPath;
import org.density.index.store.remote.filecache.FileCache;
import org.density.index.store.remote.filecache.FileCacheFactory;
import org.density.index.store.remote.filecache.FileCacheTests;
import org.density.indices.IndicesService;
import org.density.node.Node;
import org.density.test.DensityTestCase;
import org.density.transport.TransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportClearIndicesCacheActionTests extends DensityTestCase {

    private final Node testNode = mock(Node.class);
    private final TransportClearIndicesCacheAction action = new TransportClearIndicesCacheAction(
        mock(ClusterService.class),
        mock(TransportService.class),
        mock(IndicesService.class),
        testNode,
        mock(ActionFilters.class),
        mock(IndexNameExpressionResolver.class)
    );

    private final ClusterBlock writeClusterBlock = new ClusterBlock(
        1,
        "uuid",
        "",
        true,
        true,
        true,
        RestStatus.OK,
        EnumSet.of(ClusterBlockLevel.METADATA_WRITE)
    );

    private final ClusterBlock readClusterBlock = new ClusterBlock(
        1,
        "uuid",
        "",
        true,
        true,
        true,
        RestStatus.OK,
        EnumSet.of(ClusterBlockLevel.METADATA_READ)
    );

    public void testOnShardOperation() throws IOException {
        final String indexName = "test";
        final Settings settings = buildEnvSettings(Settings.EMPTY);
        final Environment environment = TestEnvironment.newEnvironment(settings);
        try (final NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, environment)) {
            // Initialize necessary stubs for the filecache clear shard operation
            final ShardId shardId = new ShardId(indexName, indexName, 1);
            final ShardRouting shardRouting = mock(ShardRouting.class);
            when(shardRouting.shardId()).thenReturn(shardId);
            final ShardPath shardPath = ShardPath.loadFileCachePath(nodeEnvironment, shardId);
            final Path cacheEntryPath = shardPath.getDataPath();
            final FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(1024 * 1024, 16, new NoopCircuitBreaker(""));

            when(testNode.fileCache()).thenReturn(fileCache);
            when(testNode.getNodeEnvironment()).thenReturn(nodeEnvironment);

            // Add an entry into the filecache and reduce the ref count
            fileCache.put(cacheEntryPath, new FileCacheTests.StubCachedIndexInput(1));
            fileCache.decRef(cacheEntryPath);

            // Check if the entry exists and reduce the ref count to make it evictable
            assertNotNull(fileCache.get(cacheEntryPath));
            fileCache.decRef(cacheEntryPath);

            ClearIndicesCacheRequest clearIndicesCacheRequest = new ClearIndicesCacheRequest();
            clearIndicesCacheRequest.fileCache(true);
            assertEquals(
                TransportBroadcastByNodeAction.EmptyResult.INSTANCE,
                action.shardOperation(clearIndicesCacheRequest, shardRouting)
            );
            assertNull(fileCache.get(cacheEntryPath));
        }
    }

    public void testGlobalBlockCheck() {
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addGlobalBlock(writeClusterBlock);
        ClusterState metadataWriteBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNull(action.checkGlobalBlock(metadataWriteBlockedState, new ClearIndicesCacheRequest()));

        builder = ClusterBlocks.builder();
        builder.addGlobalBlock(readClusterBlock);
        ClusterState metadataReadBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNotNull(action.checkGlobalBlock(metadataReadBlockedState, new ClearIndicesCacheRequest()));
    }

    public void testIndexBlockCheck() {
        String indexName = "test";
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addIndexBlock(indexName, writeClusterBlock);
        ClusterState metadataWriteBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNull(action.checkRequestBlock(metadataWriteBlockedState, new ClearIndicesCacheRequest(), new String[] { indexName }));

        builder = ClusterBlocks.builder();
        builder.addIndexBlock(indexName, readClusterBlock);
        ClusterState metadataReadBlockedState = ClusterState.builder(ClusterState.EMPTY_STATE).blocks(builder).build();
        assertNotNull(action.checkRequestBlock(metadataReadBlockedState, new ClearIndicesCacheRequest(), new String[] { indexName }));
    }
}
