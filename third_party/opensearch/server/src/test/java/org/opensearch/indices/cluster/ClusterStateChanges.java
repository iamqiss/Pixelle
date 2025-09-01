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

package org.density.indices.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.ExceptionsHelper;
import org.density.Version;
import org.density.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.density.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.density.action.admin.indices.close.CloseIndexRequest;
import org.density.action.admin.indices.close.CloseIndexResponse;
import org.density.action.admin.indices.close.TransportCloseIndexAction;
import org.density.action.admin.indices.close.TransportVerifyShardBeforeCloseAction;
import org.density.action.admin.indices.create.CreateIndexRequest;
import org.density.action.admin.indices.create.TransportCreateIndexAction;
import org.density.action.admin.indices.delete.DeleteIndexRequest;
import org.density.action.admin.indices.delete.TransportDeleteIndexAction;
import org.density.action.admin.indices.open.OpenIndexRequest;
import org.density.action.admin.indices.open.TransportOpenIndexAction;
import org.density.action.admin.indices.readonly.TransportVerifyShardIndexBlockAction;
import org.density.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.density.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.density.action.support.ActionFilters;
import org.density.action.support.DestructiveOperations;
import org.density.action.support.clustermanager.ClusterManagerNodeRequest;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.action.support.clustermanager.TransportClusterManagerNodeActionUtils;
import org.density.cluster.ClusterState;
import org.density.cluster.ClusterStateTaskExecutor;
import org.density.cluster.ClusterStateTaskExecutor.ClusterTasksResult;
import org.density.cluster.ClusterStateUpdateTask;
import org.density.cluster.EmptyClusterInfoService;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.action.shard.ShardStateAction.FailedShardEntry;
import org.density.cluster.action.shard.ShardStateAction.StartedShardEntry;
import org.density.cluster.block.ClusterBlock;
import org.density.cluster.coordination.JoinTaskExecutor;
import org.density.cluster.coordination.NodeRemovalClusterStateTaskExecutor;
import org.density.cluster.metadata.AliasValidator;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.MetadataCreateIndexService;
import org.density.cluster.metadata.MetadataDeleteIndexService;
import org.density.cluster.metadata.MetadataIndexStateService;
import org.density.cluster.metadata.MetadataIndexStateServiceUtils;
import org.density.cluster.metadata.MetadataIndexUpgradeService;
import org.density.cluster.metadata.MetadataUpdateSettingsService;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.routing.ShardRouting;
import org.density.cluster.routing.allocation.AllocationService;
import org.density.cluster.routing.allocation.AwarenessReplicaBalance;
import org.density.cluster.routing.allocation.FailedShard;
import org.density.cluster.routing.allocation.RandomAllocationDeciderTests;
import org.density.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.density.cluster.routing.allocation.decider.AllocationDeciders;
import org.density.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.density.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.density.cluster.service.ClusterService;
import org.density.common.CheckedFunction;
import org.density.common.Priority;
import org.density.common.SetOnce;
import org.density.common.UUIDs;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsModule;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.index.Index;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.env.TestEnvironment;
import org.density.index.IndexService;
import org.density.index.mapper.MapperService;
import org.density.index.mapper.MappingTransformerRegistry;
import org.density.index.shard.IndexEventListener;
import org.density.indices.DefaultRemoteStoreSettings;
import org.density.indices.IndicesService;
import org.density.indices.ShardLimitValidator;
import org.density.indices.SystemIndices;
import org.density.node.remotestore.RemoteStoreNodeService;
import org.density.repositories.RepositoriesService;
import org.density.snapshots.EmptySnapshotsInfoService;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.gateway.TestGatewayAllocator;
import org.density.threadpool.ThreadPool;
import org.density.transport.Transport;
import org.density.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.density.env.Environment.PATH_HOME_SETTING;
import static org.hamcrest.Matchers.notNullValue;
import static com.carrotsearch.randomizedtesting.RandomizedTest.getRandom;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClusterStateChanges {
    private static final Settings SETTINGS = Settings.builder().put(PATH_HOME_SETTING.getKey(), "dummy").build();

    private static final Logger logger = LogManager.getLogger(ClusterStateChanges.class);
    private final AllocationService allocationService;
    private final ClusterService clusterService;
    private final ShardStateAction.ShardFailedClusterStateTaskExecutor shardFailedClusterStateTaskExecutor;
    private final ShardStateAction.ShardStartedClusterStateTaskExecutor shardStartedClusterStateTaskExecutor;

    // transport actions
    private final TransportCloseIndexAction transportCloseIndexAction;
    private final TransportOpenIndexAction transportOpenIndexAction;
    private final TransportDeleteIndexAction transportDeleteIndexAction;
    private final TransportUpdateSettingsAction transportUpdateSettingsAction;
    private final TransportClusterRerouteAction transportClusterRerouteAction;
    private final TransportCreateIndexAction transportCreateIndexAction;
    private final RepositoriesService repositoriesService;
    private final RemoteStoreNodeService remoteStoreNodeService;

    private final NodeRemovalClusterStateTaskExecutor nodeRemovalExecutor;
    private final JoinTaskExecutor joinTaskExecutor;

    public ClusterStateChanges(NamedXContentRegistry xContentRegistry, ThreadPool threadPool) {
        ClusterSettings clusterSettings = new ClusterSettings(SETTINGS, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allocationService = new AllocationService(
            new AllocationDeciders(
                new HashSet<>(
                    Arrays.asList(
                        new SameShardAllocationDecider(SETTINGS, clusterSettings),
                        new ReplicaAfterPrimaryActiveAllocationDecider(),
                        new RandomAllocationDeciderTests.RandomAllocationDecider(getRandom())
                    )
                )
            ),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(SETTINGS),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        shardFailedClusterStateTaskExecutor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(
            allocationService,
            null,
            () -> Priority.NORMAL,
            logger
        );
        shardStartedClusterStateTaskExecutor = new ShardStateAction.ShardStartedClusterStateTaskExecutor(
            allocationService,
            null,
            () -> Priority.NORMAL,
            logger
        );
        ActionFilters actionFilters = new ActionFilters(Collections.emptySet());
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY));
        DestructiveOperations destructiveOperations = new DestructiveOperations(SETTINGS, clusterSettings);
        Environment environment = TestEnvironment.newEnvironment(SETTINGS);
        Transport transport = mock(Transport.class); // it's not used

        MappingTransformerRegistry mappingTransformerRegistry = new MappingTransformerRegistry(List.of(), xContentRegistry);

        // mocks
        clusterService = mock(ClusterService.class);
        Metadata metadata = Metadata.builder().build();
        ClusterState clusterState = ClusterState.builder(org.density.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(SETTINGS);
        IndicesService indicesService = mock(IndicesService.class);
        // MetadataCreateIndexService uses withTempIndexService to check mappings -> fake it here
        try {
            when(indicesService.withTempIndexService(any(IndexMetadata.class), any(CheckedFunction.class))).then(invocationOnMock -> {
                IndexService indexService = mock(IndexService.class);
                IndexMetadata indexMetadata = (IndexMetadata) invocationOnMock.getArguments()[0];
                when(indexService.index()).thenReturn(indexMetadata.getIndex());
                MapperService mapperService = mock(MapperService.class);
                when(indexService.mapperService()).thenReturn(mapperService);
                when(mapperService.documentMapper()).thenReturn(null);
                when(indexService.getIndexEventListener()).thenReturn(new IndexEventListener() {
                });
                when(indexService.getIndexSortSupplier()).thenReturn(() -> null);
                // noinspection unchecked
                return ((CheckedFunction) invocationOnMock.getArguments()[1]).apply(indexService);
            });
        } catch (Exception e) {
            /*
             * Catch Exception because Eclipse uses the lower bound for
             * CheckedFunction's exception type so it thinks the "when" call
             * can throw Exception. javac seems to be ok inferring something
             * else.
             */
            throw new IllegalStateException(e);
        }

        // services
        TransportService transportService = new TransportService(
            SETTINGS,
            transport,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(SETTINGS, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            clusterSettings,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        MetadataIndexUpgradeService metadataIndexUpgradeService = new MetadataIndexUpgradeService(
            SETTINGS,
            xContentRegistry,
            null,
            null,
            null,
            null
        ) {
            // metadata upgrader should do nothing
            @Override
            public IndexMetadata upgradeIndexMetadata(IndexMetadata indexMetadata, Version minimumIndexCompatibilityVersion) {
                return indexMetadata;
            }
        };

        TransportVerifyShardBeforeCloseAction transportVerifyShardBeforeCloseAction = new TransportVerifyShardBeforeCloseAction(
            SETTINGS,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            null,
            actionFilters
        );
        TransportVerifyShardIndexBlockAction transportVerifyShardIndexBlockAction = new TransportVerifyShardIndexBlockAction(
            SETTINGS,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            null,
            actionFilters
        );
        final SystemIndices systemIndices = new SystemIndices(emptyMap());
        ShardLimitValidator shardLimitValidator = new ShardLimitValidator(SETTINGS, clusterService, systemIndices);
        MetadataIndexStateService indexStateService = new MetadataIndexStateService(
            clusterService,
            allocationService,
            metadataIndexUpgradeService,
            indicesService,
            shardLimitValidator,
            threadPool,
            transportVerifyShardBeforeCloseAction,
            transportVerifyShardIndexBlockAction
        );
        MetadataDeleteIndexService deleteIndexService = new MetadataDeleteIndexService(SETTINGS, clusterService, allocationService);

        final AwarenessReplicaBalance awarenessReplicaBalance = new AwarenessReplicaBalance(SETTINGS, clusterService.getClusterSettings());

        // build IndexScopedSettings from a settingsModule so that all settings gated by enabled featureFlags are registered.
        SettingsModule settingsModule = new SettingsModule(Settings.EMPTY);

        MetadataUpdateSettingsService metadataUpdateSettingsService = new MetadataUpdateSettingsService(
            clusterService,
            allocationService,
            settingsModule.getIndexScopedSettings(),
            indicesService,
            shardLimitValidator,
            threadPool,
            awarenessReplicaBalance
        );
        MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(
            SETTINGS,
            clusterService,
            indicesService,
            allocationService,
            new AliasValidator(),
            shardLimitValidator,
            environment,
            settingsModule.getIndexScopedSettings(),
            threadPool,
            xContentRegistry,
            systemIndices,
            true,
            awarenessReplicaBalance,
            DefaultRemoteStoreSettings.INSTANCE,
            null
        );

        transportCloseIndexAction = new TransportCloseIndexAction(
            SETTINGS,
            transportService,
            clusterService,
            threadPool,
            indexStateService,
            clusterSettings,
            actionFilters,
            indexNameExpressionResolver,
            destructiveOperations
        );
        transportOpenIndexAction = new TransportOpenIndexAction(
            transportService,
            clusterService,
            threadPool,
            indexStateService,
            actionFilters,
            indexNameExpressionResolver,
            destructiveOperations
        );
        transportDeleteIndexAction = new TransportDeleteIndexAction(
            transportService,
            clusterService,
            threadPool,
            deleteIndexService,
            actionFilters,
            indexNameExpressionResolver,
            destructiveOperations
        );
        transportUpdateSettingsAction = new TransportUpdateSettingsAction(
            transportService,
            clusterService,
            threadPool,
            metadataUpdateSettingsService,
            actionFilters,
            indexNameExpressionResolver
        );
        transportClusterRerouteAction = new TransportClusterRerouteAction(
            transportService,
            clusterService,
            threadPool,
            allocationService,
            actionFilters,
            indexNameExpressionResolver
        );
        transportCreateIndexAction = new TransportCreateIndexAction(
            transportService,
            clusterService,
            threadPool,
            createIndexService,
            actionFilters,
            indexNameExpressionResolver,
            mappingTransformerRegistry
        );

        repositoriesService = new RepositoriesService(
            Settings.EMPTY,
            clusterService,
            transportService,
            Collections.emptyMap(),
            Collections.emptyMap(),
            threadPool
        );

        remoteStoreNodeService = new RemoteStoreNodeService(new SetOnce<>(repositoriesService)::get, threadPool);

        nodeRemovalExecutor = new NodeRemovalClusterStateTaskExecutor(allocationService, logger);
        joinTaskExecutor = new JoinTaskExecutor(Settings.EMPTY, allocationService, logger, (s, p, r) -> {}, remoteStoreNodeService);
    }

    public ClusterState createIndex(ClusterState state, CreateIndexRequest request) {
        return execute(transportCreateIndexAction, request, state);
    }

    public ClusterState closeIndices(ClusterState state, CloseIndexRequest request) {
        final Index[] concreteIndices = Arrays.stream(request.indices())
            .map(index -> state.metadata().index(index).getIndex())
            .toArray(Index[]::new);

        final Map<Index, ClusterBlock> blockedIndices = new HashMap<>();
        ClusterState newState = MetadataIndexStateServiceUtils.addIndexClosedBlocks(concreteIndices, blockedIndices, state);

        newState = MetadataIndexStateServiceUtils.closeRoutingTable(
            newState,
            blockedIndices,
            blockedIndices.keySet().stream().collect(Collectors.toMap(Function.identity(), CloseIndexResponse.IndexResult::new))
        );
        return allocationService.reroute(newState, "indices closed");
    }

    public ClusterState openIndices(ClusterState state, OpenIndexRequest request) {
        return execute(transportOpenIndexAction, request, state);
    }

    public ClusterState deleteIndices(ClusterState state, DeleteIndexRequest request) {
        return execute(transportDeleteIndexAction, request, state);
    }

    public ClusterState updateSettings(ClusterState state, UpdateSettingsRequest request) {
        return execute(transportUpdateSettingsAction, request, state);
    }

    public ClusterState reroute(ClusterState state, ClusterRerouteRequest request) {
        return execute(transportClusterRerouteAction, request, state);
    }

    public ClusterState addNodes(ClusterState clusterState, List<DiscoveryNode> nodes) {
        return runTasks(
            joinTaskExecutor,
            clusterState,
            nodes.stream().map(node -> new JoinTaskExecutor.Task(node, "dummy reason")).collect(Collectors.toList())
        );
    }

    public ClusterState joinNodesAndBecomeClusterManager(ClusterState clusterState, List<DiscoveryNode> nodes) {
        List<JoinTaskExecutor.Task> joinNodes = new ArrayList<>();
        joinNodes.add(JoinTaskExecutor.newBecomeClusterManagerTask());
        joinNodes.add(JoinTaskExecutor.newFinishElectionTask());
        joinNodes.addAll(nodes.stream().map(node -> new JoinTaskExecutor.Task(node, "dummy reason")).collect(Collectors.toList()));

        return runTasks(joinTaskExecutor, clusterState, joinNodes);
    }

    public ClusterState removeNodes(ClusterState clusterState, List<DiscoveryNode> nodes) {
        return runTasks(
            nodeRemovalExecutor,
            clusterState,
            nodes.stream().map(n -> new NodeRemovalClusterStateTaskExecutor.Task(n, "dummy reason")).collect(Collectors.toList())
        );
    }

    public ClusterState applyFailedShards(ClusterState clusterState, List<FailedShard> failedShards) {
        List<FailedShardEntry> entries = failedShards.stream()
            .map(
                failedShard -> new FailedShardEntry(
                    failedShard.getRoutingEntry().shardId(),
                    failedShard.getRoutingEntry().allocationId().getId(),
                    0L,
                    failedShard.getMessage(),
                    failedShard.getFailure(),
                    failedShard.markAsStale()
                )
            )
            .collect(Collectors.toList());
        return runTasks(shardFailedClusterStateTaskExecutor, clusterState, entries);
    }

    public ClusterState applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards) {
        final Map<ShardRouting, Long> entries = startedShards.stream().collect(Collectors.toMap(Function.identity(), startedShard -> {
            final IndexMetadata indexMetadata = clusterState.metadata().index(startedShard.shardId().getIndex());
            return indexMetadata != null ? indexMetadata.primaryTerm(startedShard.shardId().id()) : 0L;
        }));
        return applyStartedShards(clusterState, entries);
    }

    public ClusterState applyStartedShards(ClusterState clusterState, Map<ShardRouting, Long> startedShards) {
        return runTasks(
            shardStartedClusterStateTaskExecutor,
            clusterState,
            startedShards.entrySet()
                .stream()
                .map(e -> new StartedShardEntry(e.getKey().shardId(), e.getKey().allocationId().getId(), e.getValue(), "shard started"))
                .collect(Collectors.toList())
        );
    }

    private <T> ClusterState runTasks(ClusterStateTaskExecutor<T> executor, ClusterState clusterState, List<T> entries) {
        try {
            ClusterTasksResult<T> result = executor.execute(clusterState, entries);
            for (ClusterStateTaskExecutor.TaskResult taskResult : result.executionResults.values()) {
                if (taskResult.isSuccess() == false) {
                    throw taskResult.getFailure();
                }
            }
            return result.resultingState;
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }
    }

    private <Request extends ClusterManagerNodeRequest<Request>, Response extends ActionResponse> ClusterState execute(
        TransportClusterManagerNodeAction<Request, Response> masterNodeAction,
        Request request,
        ClusterState clusterState
    ) {
        return executeClusterStateUpdateTask(clusterState, () -> {
            try {
                TransportClusterManagerNodeActionUtils.runClusterManagerOperation(
                    masterNodeAction,
                    request,
                    clusterState,
                    new ActionListener<Response>() {
                        @Override
                        public void onResponse(Response response) {

                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new RuntimeException(e.getMessage(), e);
                        }
                    }
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ClusterState executeClusterStateUpdateTask(ClusterState state, Runnable runnable) {
        ClusterState[] result = new ClusterState[1];
        doAnswer(invocationOnMock -> {
            ClusterStateUpdateTask task = (ClusterStateUpdateTask) invocationOnMock.getArguments()[1];
            result[0] = task.execute(state);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any(ClusterStateUpdateTask.class));
        runnable.run();
        assertThat(result[0], notNullValue());
        return result[0];
    }
}
