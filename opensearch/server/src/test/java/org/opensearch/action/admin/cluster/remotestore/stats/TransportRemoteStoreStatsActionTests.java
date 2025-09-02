/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.stats;

import org.density.Version;
import org.density.action.support.ActionFilters;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.PlainShardsIterator;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.routing.ShardsIterator;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.index.Index;
import org.density.index.IndexService;
import org.density.index.IndexSettings;
import org.density.index.remote.RemoteSegmentTransferTracker;
import org.density.index.remote.RemoteStoreStatsTrackerFactory;
import org.density.index.shard.IndexShardTestCase;
import org.density.indices.IndicesService;
import org.density.indices.replication.common.ReplicationType;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.transport.MockTransport;
import org.density.transport.TransportService;

import java.util.Collections;
import java.util.stream.Collectors;

import org.mockito.Mockito;

import static org.density.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.density.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY;
import static org.density.cluster.metadata.IndexMetadata.SETTING_REMOTE_STORE_ENABLED;
import static org.density.cluster.metadata.IndexMetadata.SETTING_REPLICATION_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TransportRemoteStoreStatsActionTests extends IndexShardTestCase {
    private IndicesService indicesService;
    private RemoteStoreStatsTrackerFactory remoteStoreStatsTrackerFactory;
    private IndexMetadata remoteStoreIndexMetadata;
    private TransportService transportService;
    private ClusterService clusterService;
    private TransportRemoteStoreStatsAction statsAction;
    private DiscoveryNode localNode;
    private static final Index INDEX = new Index("testIndex", "testUUID");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indicesService = mock(IndicesService.class);
        IndexService indexService = mock(IndexService.class);
        clusterService = mock(ClusterService.class);
        remoteStoreStatsTrackerFactory = mock(RemoteStoreStatsTrackerFactory.class);
        MockTransport mockTransport = new MockTransport();
        localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        remoteStoreIndexMetadata = IndexMetadata.builder(INDEX.getName())
            .settings(
                settings(Version.CURRENT).put(SETTING_INDEX_UUID, INDEX.getUUID())
                    .put(SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    .put(SETTING_REMOTE_STORE_ENABLED, true)
                    .put(SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "my-test-repo")
                    .build()
            )
            .numberOfShards(2)
            .numberOfReplicas(1)
            .build();
        transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );

        when(remoteStoreStatsTrackerFactory.getRemoteSegmentTransferTracker(any())).thenReturn(mock(RemoteSegmentTransferTracker.class));
        when(indicesService.indexService(INDEX)).thenReturn(indexService);
        when(indexService.getIndexSettings()).thenReturn(new IndexSettings(remoteStoreIndexMetadata, Settings.EMPTY));
        statsAction = new TransportRemoteStoreStatsAction(
            clusterService,
            transportService,
            indicesService,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            remoteStoreStatsTrackerFactory
        );

    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        transportService.close();
        indicesService.close();
        clusterService.close();
    }

    public void testAllShardCopies() throws Exception {
        RoutingTable routingTable = RoutingTable.builder().addAsNew(remoteStoreIndexMetadata).build();
        Metadata metadata = Metadata.builder().put(remoteStoreIndexMetadata, false).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(clusterService.state()).thenReturn(clusterState);

        ShardsIterator shardsIterator = statsAction.shards(
            clusterService.state(),
            new RemoteStoreStatsRequest(),
            new String[] { INDEX.getName() }
        );

        assertEquals(shardsIterator.size(), 4);
    }

    public void testOnlyLocalShards() throws Exception {
        String[] concreteIndices = new String[] { INDEX.getName() };
        RoutingTable routingTable = spy(RoutingTable.builder().addAsNew(remoteStoreIndexMetadata).build());
        doReturn(new PlainShardsIterator(routingTable.allShards(INDEX.getName()).stream().map(Mockito::spy).collect(Collectors.toList())))
            .when(routingTable)
            .allShards(concreteIndices);
        routingTable.allShards(concreteIndices)
            .forEach(
                shardRouting -> doReturn(shardRouting.shardId().id() == 0 ? "node1" : localNode.getId()).when(shardRouting).currentNodeId()
            );
        Metadata metadata = Metadata.builder().put(remoteStoreIndexMetadata, false).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(clusterService.state()).thenReturn(clusterState);
        RemoteStoreStatsRequest remoteStoreStatsRequest = new RemoteStoreStatsRequest();
        remoteStoreStatsRequest.local(true);
        ShardsIterator shardsIterator = statsAction.shards(clusterService.state(), remoteStoreStatsRequest, concreteIndices);

        assertEquals(shardsIterator.size(), 2);
    }

    public void testOnlyRemoteStoreEnabledShardCopies() throws Exception {
        Index NEW_INDEX = new Index("newIndex", "newUUID");
        IndexMetadata indexMetadataWithoutRemoteStore = IndexMetadata.builder(NEW_INDEX.getName())
            .settings(
                settings(Version.CURRENT).put(SETTING_INDEX_UUID, NEW_INDEX.getUUID()).put(SETTING_REMOTE_STORE_ENABLED, false).build()
            )
            .numberOfShards(2)
            .numberOfReplicas(1)
            .build();

        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(remoteStoreIndexMetadata)
            .addAsNew(indexMetadataWithoutRemoteStore)
            .build();
        Metadata metadata = Metadata.builder().put(remoteStoreIndexMetadata, false).put(indexMetadataWithoutRemoteStore, false).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        IndexService newIndexService = mock(IndexService.class);

        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(clusterService.state()).thenReturn(clusterState);
        when(indicesService.indexService(NEW_INDEX)).thenReturn(newIndexService);
        when(newIndexService.getIndexSettings()).thenReturn(new IndexSettings(indexMetadataWithoutRemoteStore, Settings.EMPTY));

        ShardsIterator shardsIterator = statsAction.shards(
            clusterService.state(),
            new RemoteStoreStatsRequest(),
            new String[] { INDEX.getName() }
        );

        assertEquals(shardsIterator.size(), 4);
    }
}
