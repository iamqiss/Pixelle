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

package org.density.transport.client;

import org.density.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.density.action.admin.cluster.allocation.ClusterAllocationExplainRequestBuilder;
import org.density.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequestBuilder;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequestBuilder;
import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateResponse;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionRequestBuilder;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.density.action.admin.cluster.health.ClusterHealthRequest;
import org.density.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.density.action.admin.cluster.health.ClusterHealthResponse;
import org.density.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.density.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.density.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.density.action.admin.cluster.node.info.NodesInfoRequest;
import org.density.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.density.action.admin.cluster.node.info.NodesInfoResponse;
import org.density.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequestBuilder;
import org.density.action.admin.cluster.node.stats.NodesStatsRequest;
import org.density.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.density.action.admin.cluster.node.stats.NodesStatsResponse;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.density.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.density.action.admin.cluster.node.tasks.get.GetTaskRequestBuilder;
import org.density.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.density.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.density.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.density.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.density.action.admin.cluster.node.usage.NodesUsageRequest;
import org.density.action.admin.cluster.node.usage.NodesUsageRequestBuilder;
import org.density.action.admin.cluster.node.usage.NodesUsageResponse;
import org.density.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataRequest;
import org.density.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataRequestBuilder;
import org.density.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.density.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.density.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequest;
import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequestBuilder;
import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.density.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.density.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequestBuilder;
import org.density.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.density.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.density.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.density.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.density.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.density.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.density.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.density.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.density.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.density.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.density.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.density.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.density.action.admin.cluster.shards.ClusterSearchShardsRequestBuilder;
import org.density.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.density.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.density.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequestBuilder;
import org.density.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingResponse;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequest;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequestBuilder;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingResponse;
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequestBuilder;
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingResponse;
import org.density.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.density.action.admin.cluster.snapshots.clone.CloneSnapshotRequestBuilder;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.density.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.density.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.density.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.density.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.density.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.density.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.density.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.density.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.density.action.admin.cluster.state.ClusterStateRequest;
import org.density.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.density.action.admin.cluster.state.ClusterStateResponse;
import org.density.action.admin.cluster.stats.ClusterStatsRequest;
import org.density.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.density.action.admin.cluster.stats.ClusterStatsResponse;
import org.density.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.density.action.admin.cluster.storedscripts.DeleteStoredScriptRequestBuilder;
import org.density.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.density.action.admin.cluster.storedscripts.GetStoredScriptRequestBuilder;
import org.density.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.density.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.density.action.admin.cluster.storedscripts.PutStoredScriptRequestBuilder;
import org.density.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.density.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import org.density.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.density.action.admin.cluster.wlm.WlmStatsRequest;
import org.density.action.admin.cluster.wlm.WlmStatsResponse;
import org.density.action.admin.indices.dangling.delete.DeleteDanglingIndexRequest;
import org.density.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.density.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.density.action.admin.indices.dangling.list.ListDanglingIndicesResponse;
import org.density.action.ingest.DeletePipelineRequest;
import org.density.action.ingest.DeletePipelineRequestBuilder;
import org.density.action.ingest.GetPipelineRequest;
import org.density.action.ingest.GetPipelineRequestBuilder;
import org.density.action.ingest.GetPipelineResponse;
import org.density.action.ingest.PutPipelineRequest;
import org.density.action.ingest.PutPipelineRequestBuilder;
import org.density.action.ingest.SimulatePipelineRequest;
import org.density.action.ingest.SimulatePipelineRequestBuilder;
import org.density.action.ingest.SimulatePipelineResponse;
import org.density.action.search.DeleteSearchPipelineRequest;
import org.density.action.search.GetSearchPipelineRequest;
import org.density.action.search.GetSearchPipelineResponse;
import org.density.action.search.PutSearchPipelineRequest;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.common.action.ActionFuture;
import org.density.common.annotation.PublicApi;
import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesReference;
import org.density.core.tasks.TaskId;
import org.density.core.xcontent.MediaType;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#cluster()
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public interface ClusterAdminClient extends DensityClient {

    /**
     * The health of the cluster.
     *
     * @param request The cluster state request
     * @return The result future
     * @see Requests#clusterHealthRequest(String...)
     */
    ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request);

    /**
     * The health of the cluster.
     *
     * @param request  The cluster state request
     * @param listener A listener to be notified with a result
     * @see Requests#clusterHealthRequest(String...)
     */
    void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener);

    /**
     * The health of the cluster.
     */
    ClusterHealthRequestBuilder prepareHealth(String... indices);

    /**
     * The state of the cluster.
     *
     * @param request The cluster state request.
     * @return The result future
     * @see Requests#clusterStateRequest()
     */
    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request);

    /**
     * The state of the cluster.
     *
     * @param request  The cluster state request.
     * @param listener A listener to be notified with a result
     * @see Requests#clusterStateRequest()
     */
    void state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener);

    /**
     * The state of the cluster.
     */
    ClusterStateRequestBuilder prepareState();

    /**
     * Updates settings in the cluster.
     */
    ActionFuture<ClusterUpdateSettingsResponse> updateSettings(ClusterUpdateSettingsRequest request);

    /**
     * Update settings in the cluster.
     */
    void updateSettings(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterUpdateSettingsRequestBuilder prepareUpdateSettings();

    /**
     * Re initialize each cluster node and pass them the secret store password.
     */
    NodesReloadSecureSettingsRequestBuilder prepareReloadSecureSettings();

    /**
     * Reroutes allocation of shards. Advance API.
     */
    ActionFuture<ClusterRerouteResponse> reroute(ClusterRerouteRequest request);

    /**
     * Reroutes allocation of shards. Advance API.
     */
    void reroute(ClusterRerouteRequest request, ActionListener<ClusterRerouteResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterRerouteRequestBuilder prepareReroute();

    /**
     * Nodes info of the cluster.
     *
     * @param request The nodes info request
     * @return The result future
     * @see Requests#nodesInfoRequest(String...)
     */
    ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request);

    /**
     * Nodes info of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see Requests#nodesInfoRequest(String...)
     */
    void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);

    /**
     * Nodes info of the cluster.
     */
    NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds);

    /**
     * Cluster wide aggregated stats.
     *
     * @param request The cluster stats request
     * @return The result future
     * @see Requests#clusterStatsRequest
     */
    ActionFuture<ClusterStatsResponse> clusterStats(ClusterStatsRequest request);

    /**
     * Cluster wide aggregated stats
     *
     * @param request  The cluster stats request
     * @param listener A listener to be notified with a result
     * @see Requests#clusterStatsRequest()
     */
    void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener);

    ClusterStatsRequestBuilder prepareClusterStats();

    /**
     * Nodes stats of the cluster.
     *
     * @param request The nodes stats request
     * @return The result future
     * @see Requests#nodesStatsRequest(String...)
     */
    ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request);

    /**
     * Nodes stats of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see Requests#nodesStatsRequest(String...)
     */
    void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);

    /**
     * Nodes stats of the cluster.
     */
    NodesStatsRequestBuilder prepareNodesStats(String... nodesIds);

    /**
     * WorkloadGroup stats of the cluster.
     * @param request The wlmStatsRequest
     * @param listener A listener to be notified with a result
     */
    void wlmStats(WlmStatsRequest request, ActionListener<WlmStatsResponse> listener);

    void remoteStoreStats(RemoteStoreStatsRequest request, ActionListener<RemoteStoreStatsResponse> listener);

    RemoteStoreStatsRequestBuilder prepareRemoteStoreStats(String index, String shardId);

    void remoteStoreMetadata(RemoteStoreMetadataRequest request, ActionListener<RemoteStoreMetadataResponse> listener);

    RemoteStoreMetadataRequestBuilder prepareRemoteStoreMetadata(String index, String shardId);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request. Nodes usage of the
     * cluster.
     *
     * @param request
     *            The nodes usage request
     * @return The result future
     * @see Requests#nodesUsageRequest(String...)
     */
    ActionFuture<NodesUsageResponse> nodesUsage(NodesUsageRequest request);

    /**
     * Nodes usage of the cluster.
     *
     * @param request
     *            The nodes usage request
     * @param listener
     *            A listener to be notified with a result
     * @see Requests#nodesUsageRequest(String...)
     */
    void nodesUsage(NodesUsageRequest request, ActionListener<NodesUsageResponse> listener);

    /**
     * Nodes usage of the cluster.
     */
    NodesUsageRequestBuilder prepareNodesUsage(String... nodesIds);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request.
     *
     */
    ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids specified in the request.
     */
    void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener);

    /**
     * Returns a request builder to fetch top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids provided. Note: Use {@code *} to fetch samples for all nodes
     */
    NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds);

    /**
     * List tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     * @see Requests#listTasksRequest()
     */
    ActionFuture<ListTasksResponse> listTasks(ListTasksRequest request);

    /**
     * List active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     * @see Requests#listTasksRequest()
     */
    void listTasks(ListTasksRequest request, ActionListener<ListTasksResponse> listener);

    /**
     * List active tasks
     */
    ListTasksRequestBuilder prepareListTasks(String... nodesIds);

    /**
     * Get a task.
     *
     * @param request the request
     * @return the result future
     * @see Requests#getTaskRequest()
     */
    ActionFuture<GetTaskResponse> getTask(GetTaskRequest request);

    /**
     * Get a task.
     *
     * @param request the request
     * @param listener A listener to be notified with the result
     * @see Requests#getTaskRequest()
     */
    void getTask(GetTaskRequest request, ActionListener<GetTaskResponse> listener);

    /**
     * Fetch a task by id.
     */
    GetTaskRequestBuilder prepareGetTask(String taskId);

    /**
     * Fetch a task by id.
     */
    GetTaskRequestBuilder prepareGetTask(TaskId taskId);

    /**
     * Cancel tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     * @see Requests#cancelTasksRequest()
     */
    ActionFuture<CancelTasksResponse> cancelTasks(CancelTasksRequest request);

    /**
     * Cancel active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     * @see Requests#cancelTasksRequest()
     */
    void cancelTasks(CancelTasksRequest request, ActionListener<CancelTasksResponse> listener);

    /**
     * Cancel active tasks
     */
    CancelTasksRequestBuilder prepareCancelTasks(String... nodesIds);

    /**
     * Returns list of shards the given search would be executed on.
     */
    ActionFuture<ClusterSearchShardsResponse> searchShards(ClusterSearchShardsRequest request);

    /**
     * Returns list of shards the given search would be executed on.
     */
    void searchShards(ClusterSearchShardsRequest request, ActionListener<ClusterSearchShardsResponse> listener);

    /**
     * Returns list of shards the given search would be executed on.
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards();

    /**
     * Returns list of shards the given search would be executed on.
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices);

    /**
     * Registers a snapshot repository.
     */
    ActionFuture<AcknowledgedResponse> putRepository(PutRepositoryRequest request);

    /**
     * Registers a snapshot repository.
     */
    void putRepository(PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Registers a snapshot repository.
     */
    PutRepositoryRequestBuilder preparePutRepository(String name);

    /**
     * Unregisters a repository.
     */
    ActionFuture<AcknowledgedResponse> deleteRepository(DeleteRepositoryRequest request);

    /**
     * Unregisters a repository.
     */
    void deleteRepository(DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Unregisters a repository.
     */
    DeleteRepositoryRequestBuilder prepareDeleteRepository(String name);

    /**
     * Gets repositories.
     */
    ActionFuture<GetRepositoriesResponse> getRepositories(GetRepositoriesRequest request);

    /**
     * Gets repositories.
     */
    void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener);

    /**
     * Gets repositories.
     */
    GetRepositoriesRequestBuilder prepareGetRepositories(String... name);

    /**
     * Cleans up repository.
     */
    CleanupRepositoryRequestBuilder prepareCleanupRepository(String repository);

    /**
     * Cleans up repository.
     */
    ActionFuture<CleanupRepositoryResponse> cleanupRepository(CleanupRepositoryRequest repository);

    /**
     * Cleans up repository.
     */
    void cleanupRepository(CleanupRepositoryRequest repository, ActionListener<CleanupRepositoryResponse> listener);

    /**
     * Verifies a repository.
     */
    ActionFuture<VerifyRepositoryResponse> verifyRepository(VerifyRepositoryRequest request);

    /**
     * Verifies a repository.
     */
    void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener);

    /**
     * Verifies a repository.
     */
    VerifyRepositoryRequestBuilder prepareVerifyRepository(String name);

    /**
     * Creates a new snapshot.
     */
    ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request);

    /**
     * Creates a new snapshot.
     */
    void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener);

    /**
     * Creates a new snapshot.
     */
    CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name);

    /**
     * Clones a snapshot.
     */
    CloneSnapshotRequestBuilder prepareCloneSnapshot(String repository, String source, String target);

    /**
     * Clones a snapshot.
     */
    ActionFuture<AcknowledgedResponse> cloneSnapshot(CloneSnapshotRequest request);

    /**
     * Clones a snapshot.
     */
    void cloneSnapshot(CloneSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Get snapshots.
     */
    ActionFuture<GetSnapshotsResponse> getSnapshots(GetSnapshotsRequest request);

    /**
     * Get snapshot.
     */
    void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener);

    /**
     * Get snapshot.
     */
    GetSnapshotsRequestBuilder prepareGetSnapshots(String repository);

    /**
     * Delete snapshot.
     */
    ActionFuture<AcknowledgedResponse> deleteSnapshot(DeleteSnapshotRequest request);

    /**
     * Delete snapshot.
     */
    void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete snapshot.
     */
    DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String... snapshot);

    /**
     * Restores a snapshot.
     */
    ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request);

    /**
     * Restores a snapshot.
     */
    void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener);

    /**
     * Restores from remote store.
     */
    void restoreRemoteStore(RestoreRemoteStoreRequest request, ActionListener<RestoreRemoteStoreResponse> listener);

    /**
     * Restores a snapshot.
     */
    RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    void pendingClusterTasks(PendingClusterTasksRequest request, ActionListener<PendingClusterTasksResponse> listener);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    ActionFuture<PendingClusterTasksResponse> pendingClusterTasks(PendingClusterTasksRequest request);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    PendingClusterTasksRequestBuilder preparePendingClusterTasks();

    /**
     * Get snapshot status.
     */
    ActionFuture<SnapshotsStatusResponse> snapshotsStatus(SnapshotsStatusRequest request);

    /**
     * Get snapshot status.
     */
    void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener);

    /**
     * Get snapshot status.
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository);

    /**
     * Get snapshot status.
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus();

    /**
     * Stores an ingest pipeline
     */
    void putPipeline(PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Stores an ingest pipeline
     */
    ActionFuture<AcknowledgedResponse> putPipeline(PutPipelineRequest request);

    /**
     * Stores an ingest pipeline
     */
    PutPipelineRequestBuilder preparePutPipeline(String id, BytesReference source, MediaType mediaType);

    /**
     * Deletes a stored ingest pipeline
     */
    void deletePipeline(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes a stored ingest pipeline
     */
    ActionFuture<AcknowledgedResponse> deletePipeline(DeletePipelineRequest request);

    /**
     * Deletes a stored ingest pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline();

    /**
     * Deletes a stored ingest pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline(String id);

    /**
     * Returns a stored ingest pipeline
     */
    void getPipeline(GetPipelineRequest request, ActionListener<GetPipelineResponse> listener);

    /**
     * Returns a stored ingest pipeline
     */
    ActionFuture<GetPipelineResponse> getPipeline(GetPipelineRequest request);

    /**
     * Returns a stored ingest pipeline
     */
    GetPipelineRequestBuilder prepareGetPipeline(String... ids);

    /**
     * Simulates an ingest pipeline
     */
    void simulatePipeline(SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener);

    /**
     * Simulates an ingest pipeline
     */
    ActionFuture<SimulatePipelineResponse> simulatePipeline(SimulatePipelineRequest request);

    /**
     * Simulates an ingest pipeline
     */
    SimulatePipelineRequestBuilder prepareSimulatePipeline(BytesReference source, MediaType mediaType);

    /**
     * Explain the allocation of a shard
     */
    void allocationExplain(ClusterAllocationExplainRequest request, ActionListener<ClusterAllocationExplainResponse> listener);

    /**
     * Explain the allocation of a shard
     */
    ActionFuture<ClusterAllocationExplainResponse> allocationExplain(ClusterAllocationExplainRequest request);

    /**
     * Explain the allocation of a shard
     */
    ClusterAllocationExplainRequestBuilder prepareAllocationExplain();

    /**
     * Store a script in the cluster state
     */
    PutStoredScriptRequestBuilder preparePutStoredScript();

    /**
     * Delete a script from the cluster state
     */
    void deleteStoredScript(DeleteStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete a script from the cluster state
     */
    ActionFuture<AcknowledgedResponse> deleteStoredScript(DeleteStoredScriptRequest request);

    /**
     * Delete a script from the cluster state
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript();

    /**
     * Delete a script from the cluster state
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript(String id);

    /**
     * Store a script in the cluster state
     */
    void putStoredScript(PutStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Store a script in the cluster state
     */
    ActionFuture<AcknowledgedResponse> putStoredScript(PutStoredScriptRequest request);

    /**
     * Get a script from the cluster state
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript();

    /**
     * Get a script from the cluster state
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript(String id);

    /**
     * Get a script from the cluster state
     */
    void getStoredScript(GetStoredScriptRequest request, ActionListener<GetStoredScriptResponse> listener);

    /**
     * Get a script from the cluster state
     */
    ActionFuture<GetStoredScriptResponse> getStoredScript(GetStoredScriptRequest request);

    /**
     * List dangling indices on all nodes.
     */
    void listDanglingIndices(ListDanglingIndicesRequest request, ActionListener<ListDanglingIndicesResponse> listener);

    /**
     * List dangling indices on all nodes.
     */
    ActionFuture<ListDanglingIndicesResponse> listDanglingIndices(ListDanglingIndicesRequest request);

    /**
     * Restore specified dangling indices.
     */
    void importDanglingIndex(ImportDanglingIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Restore specified dangling indices.
     */
    ActionFuture<AcknowledgedResponse> importDanglingIndex(ImportDanglingIndexRequest request);

    /**
     * Delete specified dangling indices.
     */
    void deleteDanglingIndex(DeleteDanglingIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete specified dangling indices.
     */
    ActionFuture<AcknowledgedResponse> deleteDanglingIndex(DeleteDanglingIndexRequest request);

    /**
     * Updates weights for weighted round-robin search routing policy.
     */
    ActionFuture<ClusterPutWeightedRoutingResponse> putWeightedRouting(ClusterPutWeightedRoutingRequest request);

    /**
     * Updates weights for weighted round-robin search routing policy.
     */
    void putWeightedRouting(ClusterPutWeightedRoutingRequest request, ActionListener<ClusterPutWeightedRoutingResponse> listener);

    /**
     * Updates weights for weighted round-robin search routing policy.
     */
    ClusterPutWeightedRoutingRequestBuilder prepareWeightedRouting();

    /**
     * Gets weights for weighted round-robin search routing policy.
     */
    ActionFuture<ClusterGetWeightedRoutingResponse> getWeightedRouting(ClusterGetWeightedRoutingRequest request);

    /**
     * Gets weights for weighted round-robin search routing policy.
     */
    void getWeightedRouting(ClusterGetWeightedRoutingRequest request, ActionListener<ClusterGetWeightedRoutingResponse> listener);

    /**
     * Gets weights for weighted round-robin search routing policy.
     */
    ClusterGetWeightedRoutingRequestBuilder prepareGetWeightedRouting();

    /**
     * Deletes weights for weighted round-robin search routing policy.
     */
    ActionFuture<ClusterDeleteWeightedRoutingResponse> deleteWeightedRouting(ClusterDeleteWeightedRoutingRequest request);

    /**
     * Deletes weights for weighted round-robin search routing policy.
     */
    void deleteWeightedRouting(ClusterDeleteWeightedRoutingRequest request, ActionListener<ClusterDeleteWeightedRoutingResponse> listener);

    /**
     * Deletes weights for weighted round-robin search routing policy.
     */
    ClusterDeleteWeightedRoutingRequestBuilder prepareDeleteWeightedRouting();

    /**
     * Decommission awareness attribute
     */
    ActionFuture<DecommissionResponse> decommission(DecommissionRequest request);

    /**
     * Decommission awareness attribute
     */
    void decommission(DecommissionRequest request, ActionListener<DecommissionResponse> listener);

    /**
     * Decommission awareness attribute
     */
    DecommissionRequestBuilder prepareDecommission(DecommissionRequest request);

    /**
     * Get Decommissioned attribute
     */
    ActionFuture<GetDecommissionStateResponse> getDecommissionState(GetDecommissionStateRequest request);

    /**
     * Get Decommissioned attribute
     */
    void getDecommissionState(GetDecommissionStateRequest request, ActionListener<GetDecommissionStateResponse> listener);

    /**
     * Get Decommissioned attribute
     */
    GetDecommissionStateRequestBuilder prepareGetDecommissionState();

    /**
     * Deletes the decommission metadata.
     */
    ActionFuture<DeleteDecommissionStateResponse> deleteDecommissionState(DeleteDecommissionStateRequest request);

    /**
     * Deletes the decommission metadata.
     */
    void deleteDecommissionState(DeleteDecommissionStateRequest request, ActionListener<DeleteDecommissionStateResponse> listener);

    /**
     * Deletes the decommission metadata.
     */
    DeleteDecommissionStateRequestBuilder prepareDeleteDecommissionRequest();

    /**
     * Stores a search pipeline
     */
    void putSearchPipeline(PutSearchPipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Stores a search pipeline
     */
    ActionFuture<AcknowledgedResponse> putSearchPipeline(PutSearchPipelineRequest request);

    /**
     * Returns a stored search pipeline
     */
    void getSearchPipeline(GetSearchPipelineRequest request, ActionListener<GetSearchPipelineResponse> listener);

    /**
     * Returns a stored search pipeline
     */
    ActionFuture<GetSearchPipelineResponse> getSearchPipeline(GetSearchPipelineRequest request);

    /**
     * Deletes a stored search pipeline
     */
    void deleteSearchPipeline(DeleteSearchPipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes a stored search pipeline
     */
    ActionFuture<AcknowledgedResponse> deleteSearchPipeline(DeleteSearchPipelineRequest request);
}
