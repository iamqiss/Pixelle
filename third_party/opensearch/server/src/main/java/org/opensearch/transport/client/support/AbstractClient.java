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

package org.density.transport.client.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.ActionRequest;
import org.density.action.ActionType;
import org.density.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.density.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.density.action.admin.cluster.allocation.ClusterAllocationExplainRequestBuilder;
import org.density.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateAction;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequestBuilder;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateAction;
import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequestBuilder;
import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateResponse;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionAction;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionRequestBuilder;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.density.action.admin.cluster.health.ClusterHealthAction;
import org.density.action.admin.cluster.health.ClusterHealthRequest;
import org.density.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.density.action.admin.cluster.health.ClusterHealthResponse;
import org.density.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.density.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.density.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.density.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.density.action.admin.cluster.node.info.NodesInfoAction;
import org.density.action.admin.cluster.node.info.NodesInfoRequest;
import org.density.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.density.action.admin.cluster.node.info.NodesInfoResponse;
import org.density.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.density.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequestBuilder;
import org.density.action.admin.cluster.node.stats.NodesStatsAction;
import org.density.action.admin.cluster.node.stats.NodesStatsRequest;
import org.density.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.density.action.admin.cluster.node.stats.NodesStatsResponse;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.density.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.density.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.density.action.admin.cluster.node.tasks.get.GetTaskRequestBuilder;
import org.density.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.density.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.density.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.density.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.density.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.density.action.admin.cluster.node.usage.NodesUsageAction;
import org.density.action.admin.cluster.node.usage.NodesUsageRequest;
import org.density.action.admin.cluster.node.usage.NodesUsageRequestBuilder;
import org.density.action.admin.cluster.node.usage.NodesUsageResponse;
import org.density.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataAction;
import org.density.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataRequest;
import org.density.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataRequestBuilder;
import org.density.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.density.action.admin.cluster.remotestore.restore.RestoreRemoteStoreAction;
import org.density.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.density.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsAction;
import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequest;
import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsRequestBuilder;
import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.density.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.density.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.density.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequestBuilder;
import org.density.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.density.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.density.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.density.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.density.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.density.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.density.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.density.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.density.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.density.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.density.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.density.action.admin.cluster.reroute.ClusterRerouteAction;
import org.density.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.density.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.density.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.density.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.density.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.density.action.admin.cluster.shards.ClusterSearchShardsRequestBuilder;
import org.density.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.density.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingAction;
import org.density.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.density.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequestBuilder;
import org.density.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingResponse;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingAction;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequest;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequestBuilder;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingResponse;
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterAddWeightedRoutingAction;
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequestBuilder;
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingResponse;
import org.density.action.admin.cluster.snapshots.clone.CloneSnapshotAction;
import org.density.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.density.action.admin.cluster.snapshots.clone.CloneSnapshotRequestBuilder;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.density.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.density.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.density.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.density.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.density.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.density.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.density.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.density.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.density.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.density.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.density.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.density.action.admin.cluster.state.ClusterStateAction;
import org.density.action.admin.cluster.state.ClusterStateRequest;
import org.density.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.density.action.admin.cluster.state.ClusterStateResponse;
import org.density.action.admin.cluster.stats.ClusterStatsAction;
import org.density.action.admin.cluster.stats.ClusterStatsRequest;
import org.density.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.density.action.admin.cluster.stats.ClusterStatsResponse;
import org.density.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.density.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.density.action.admin.cluster.storedscripts.DeleteStoredScriptRequestBuilder;
import org.density.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.density.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.density.action.admin.cluster.storedscripts.GetStoredScriptRequestBuilder;
import org.density.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.density.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.density.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.density.action.admin.cluster.storedscripts.PutStoredScriptRequestBuilder;
import org.density.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.density.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.density.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import org.density.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.density.action.admin.cluster.wlm.WlmStatsAction;
import org.density.action.admin.cluster.wlm.WlmStatsRequest;
import org.density.action.admin.cluster.wlm.WlmStatsResponse;
import org.density.action.admin.indices.alias.IndicesAliasesAction;
import org.density.action.admin.indices.alias.IndicesAliasesRequest;
import org.density.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.density.action.admin.indices.alias.get.GetAliasesAction;
import org.density.action.admin.indices.alias.get.GetAliasesRequest;
import org.density.action.admin.indices.alias.get.GetAliasesRequestBuilder;
import org.density.action.admin.indices.alias.get.GetAliasesResponse;
import org.density.action.admin.indices.analyze.AnalyzeAction;
import org.density.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.density.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.density.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.density.action.admin.indices.cache.clear.ClearIndicesCacheRequestBuilder;
import org.density.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.density.action.admin.indices.close.CloseIndexAction;
import org.density.action.admin.indices.close.CloseIndexRequest;
import org.density.action.admin.indices.close.CloseIndexRequestBuilder;
import org.density.action.admin.indices.close.CloseIndexResponse;
import org.density.action.admin.indices.create.CreateIndexAction;
import org.density.action.admin.indices.create.CreateIndexRequest;
import org.density.action.admin.indices.create.CreateIndexRequestBuilder;
import org.density.action.admin.indices.create.CreateIndexResponse;
import org.density.action.admin.indices.dangling.delete.DeleteDanglingIndexAction;
import org.density.action.admin.indices.dangling.delete.DeleteDanglingIndexRequest;
import org.density.action.admin.indices.dangling.import_index.ImportDanglingIndexAction;
import org.density.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.density.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.density.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.density.action.admin.indices.dangling.list.ListDanglingIndicesResponse;
import org.density.action.admin.indices.datastream.CreateDataStreamAction;
import org.density.action.admin.indices.datastream.DeleteDataStreamAction;
import org.density.action.admin.indices.datastream.GetDataStreamAction;
import org.density.action.admin.indices.delete.DeleteIndexAction;
import org.density.action.admin.indices.delete.DeleteIndexRequest;
import org.density.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.density.action.admin.indices.exists.indices.IndicesExistsAction;
import org.density.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.density.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.density.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.density.action.admin.indices.flush.FlushAction;
import org.density.action.admin.indices.flush.FlushRequest;
import org.density.action.admin.indices.flush.FlushRequestBuilder;
import org.density.action.admin.indices.flush.FlushResponse;
import org.density.action.admin.indices.forcemerge.ForceMergeAction;
import org.density.action.admin.indices.forcemerge.ForceMergeRequest;
import org.density.action.admin.indices.forcemerge.ForceMergeRequestBuilder;
import org.density.action.admin.indices.forcemerge.ForceMergeResponse;
import org.density.action.admin.indices.get.GetIndexAction;
import org.density.action.admin.indices.get.GetIndexRequest;
import org.density.action.admin.indices.get.GetIndexRequestBuilder;
import org.density.action.admin.indices.get.GetIndexResponse;
import org.density.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.density.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.density.action.admin.indices.mapping.get.GetFieldMappingsRequestBuilder;
import org.density.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.density.action.admin.indices.mapping.get.GetMappingsAction;
import org.density.action.admin.indices.mapping.get.GetMappingsRequest;
import org.density.action.admin.indices.mapping.get.GetMappingsRequestBuilder;
import org.density.action.admin.indices.mapping.get.GetMappingsResponse;
import org.density.action.admin.indices.mapping.put.PutMappingAction;
import org.density.action.admin.indices.mapping.put.PutMappingRequest;
import org.density.action.admin.indices.mapping.put.PutMappingRequestBuilder;
import org.density.action.admin.indices.open.OpenIndexAction;
import org.density.action.admin.indices.open.OpenIndexRequest;
import org.density.action.admin.indices.open.OpenIndexRequestBuilder;
import org.density.action.admin.indices.open.OpenIndexResponse;
import org.density.action.admin.indices.readonly.AddIndexBlockAction;
import org.density.action.admin.indices.readonly.AddIndexBlockRequest;
import org.density.action.admin.indices.readonly.AddIndexBlockRequestBuilder;
import org.density.action.admin.indices.readonly.AddIndexBlockResponse;
import org.density.action.admin.indices.recovery.RecoveryAction;
import org.density.action.admin.indices.recovery.RecoveryRequest;
import org.density.action.admin.indices.recovery.RecoveryRequestBuilder;
import org.density.action.admin.indices.recovery.RecoveryResponse;
import org.density.action.admin.indices.refresh.RefreshAction;
import org.density.action.admin.indices.refresh.RefreshRequest;
import org.density.action.admin.indices.refresh.RefreshRequestBuilder;
import org.density.action.admin.indices.refresh.RefreshResponse;
import org.density.action.admin.indices.replication.SegmentReplicationStatsAction;
import org.density.action.admin.indices.replication.SegmentReplicationStatsRequest;
import org.density.action.admin.indices.replication.SegmentReplicationStatsRequestBuilder;
import org.density.action.admin.indices.replication.SegmentReplicationStatsResponse;
import org.density.action.admin.indices.resolve.ResolveIndexAction;
import org.density.action.admin.indices.rollover.RolloverAction;
import org.density.action.admin.indices.rollover.RolloverRequest;
import org.density.action.admin.indices.rollover.RolloverRequestBuilder;
import org.density.action.admin.indices.rollover.RolloverResponse;
import org.density.action.admin.indices.scale.searchonly.ScaleIndexRequestBuilder;
import org.density.action.admin.indices.segments.IndicesSegmentResponse;
import org.density.action.admin.indices.segments.IndicesSegmentsAction;
import org.density.action.admin.indices.segments.IndicesSegmentsRequest;
import org.density.action.admin.indices.segments.IndicesSegmentsRequestBuilder;
import org.density.action.admin.indices.segments.PitSegmentsAction;
import org.density.action.admin.indices.segments.PitSegmentsRequest;
import org.density.action.admin.indices.settings.get.GetSettingsAction;
import org.density.action.admin.indices.settings.get.GetSettingsRequest;
import org.density.action.admin.indices.settings.get.GetSettingsRequestBuilder;
import org.density.action.admin.indices.settings.get.GetSettingsResponse;
import org.density.action.admin.indices.settings.put.UpdateSettingsAction;
import org.density.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.density.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.density.action.admin.indices.shards.IndicesShardStoreRequestBuilder;
import org.density.action.admin.indices.shards.IndicesShardStoresAction;
import org.density.action.admin.indices.shards.IndicesShardStoresRequest;
import org.density.action.admin.indices.shards.IndicesShardStoresResponse;
import org.density.action.admin.indices.shrink.ResizeAction;
import org.density.action.admin.indices.shrink.ResizeRequest;
import org.density.action.admin.indices.shrink.ResizeRequestBuilder;
import org.density.action.admin.indices.shrink.ResizeResponse;
import org.density.action.admin.indices.stats.IndicesStatsAction;
import org.density.action.admin.indices.stats.IndicesStatsRequest;
import org.density.action.admin.indices.stats.IndicesStatsRequestBuilder;
import org.density.action.admin.indices.stats.IndicesStatsResponse;
import org.density.action.admin.indices.streamingingestion.pause.PauseIngestionAction;
import org.density.action.admin.indices.streamingingestion.pause.PauseIngestionRequest;
import org.density.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.density.action.admin.indices.streamingingestion.resume.ResumeIngestionAction;
import org.density.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.density.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.density.action.admin.indices.streamingingestion.state.GetIngestionStateAction;
import org.density.action.admin.indices.streamingingestion.state.GetIngestionStateRequest;
import org.density.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.density.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.density.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.density.action.admin.indices.template.delete.DeleteIndexTemplateRequestBuilder;
import org.density.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.density.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.density.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.density.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.density.action.admin.indices.template.put.PutIndexTemplateAction;
import org.density.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.density.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.density.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.density.action.admin.indices.upgrade.get.UpgradeStatusRequest;
import org.density.action.admin.indices.upgrade.get.UpgradeStatusRequestBuilder;
import org.density.action.admin.indices.upgrade.get.UpgradeStatusResponse;
import org.density.action.admin.indices.upgrade.post.UpgradeAction;
import org.density.action.admin.indices.upgrade.post.UpgradeRequest;
import org.density.action.admin.indices.upgrade.post.UpgradeRequestBuilder;
import org.density.action.admin.indices.upgrade.post.UpgradeResponse;
import org.density.action.admin.indices.validate.query.ValidateQueryAction;
import org.density.action.admin.indices.validate.query.ValidateQueryRequest;
import org.density.action.admin.indices.validate.query.ValidateQueryRequestBuilder;
import org.density.action.admin.indices.validate.query.ValidateQueryResponse;
import org.density.action.admin.indices.view.CreateViewAction;
import org.density.action.admin.indices.view.DeleteViewAction;
import org.density.action.admin.indices.view.GetViewAction;
import org.density.action.admin.indices.view.ListViewNamesAction;
import org.density.action.admin.indices.view.SearchViewAction;
import org.density.action.admin.indices.view.UpdateViewAction;
import org.density.action.bulk.BulkAction;
import org.density.action.bulk.BulkRequest;
import org.density.action.bulk.BulkRequestBuilder;
import org.density.action.bulk.BulkResponse;
import org.density.action.delete.DeleteAction;
import org.density.action.delete.DeleteRequest;
import org.density.action.delete.DeleteRequestBuilder;
import org.density.action.delete.DeleteResponse;
import org.density.action.explain.ExplainAction;
import org.density.action.explain.ExplainRequest;
import org.density.action.explain.ExplainRequestBuilder;
import org.density.action.explain.ExplainResponse;
import org.density.action.fieldcaps.FieldCapabilitiesAction;
import org.density.action.fieldcaps.FieldCapabilitiesRequest;
import org.density.action.fieldcaps.FieldCapabilitiesRequestBuilder;
import org.density.action.fieldcaps.FieldCapabilitiesResponse;
import org.density.action.get.GetAction;
import org.density.action.get.GetRequest;
import org.density.action.get.GetRequestBuilder;
import org.density.action.get.GetResponse;
import org.density.action.get.MultiGetAction;
import org.density.action.get.MultiGetRequest;
import org.density.action.get.MultiGetRequestBuilder;
import org.density.action.get.MultiGetResponse;
import org.density.action.index.IndexAction;
import org.density.action.index.IndexRequest;
import org.density.action.index.IndexRequestBuilder;
import org.density.action.index.IndexResponse;
import org.density.action.ingest.DeletePipelineAction;
import org.density.action.ingest.DeletePipelineRequest;
import org.density.action.ingest.DeletePipelineRequestBuilder;
import org.density.action.ingest.GetPipelineAction;
import org.density.action.ingest.GetPipelineRequest;
import org.density.action.ingest.GetPipelineRequestBuilder;
import org.density.action.ingest.GetPipelineResponse;
import org.density.action.ingest.PutPipelineAction;
import org.density.action.ingest.PutPipelineRequest;
import org.density.action.ingest.PutPipelineRequestBuilder;
import org.density.action.ingest.SimulatePipelineAction;
import org.density.action.ingest.SimulatePipelineRequest;
import org.density.action.ingest.SimulatePipelineRequestBuilder;
import org.density.action.ingest.SimulatePipelineResponse;
import org.density.action.search.ClearScrollAction;
import org.density.action.search.ClearScrollRequest;
import org.density.action.search.ClearScrollRequestBuilder;
import org.density.action.search.ClearScrollResponse;
import org.density.action.search.CreatePitAction;
import org.density.action.search.CreatePitRequest;
import org.density.action.search.CreatePitResponse;
import org.density.action.search.DeletePitAction;
import org.density.action.search.DeletePitRequest;
import org.density.action.search.DeletePitResponse;
import org.density.action.search.DeleteSearchPipelineAction;
import org.density.action.search.DeleteSearchPipelineRequest;
import org.density.action.search.GetAllPitNodesRequest;
import org.density.action.search.GetAllPitNodesResponse;
import org.density.action.search.GetAllPitsAction;
import org.density.action.search.GetSearchPipelineAction;
import org.density.action.search.GetSearchPipelineRequest;
import org.density.action.search.GetSearchPipelineResponse;
import org.density.action.search.MultiSearchAction;
import org.density.action.search.MultiSearchRequest;
import org.density.action.search.MultiSearchRequestBuilder;
import org.density.action.search.MultiSearchResponse;
import org.density.action.search.PutSearchPipelineAction;
import org.density.action.search.PutSearchPipelineRequest;
import org.density.action.search.SearchAction;
import org.density.action.search.SearchRequest;
import org.density.action.search.SearchRequestBuilder;
import org.density.action.search.SearchResponse;
import org.density.action.search.SearchScrollAction;
import org.density.action.search.SearchScrollRequest;
import org.density.action.search.SearchScrollRequestBuilder;
import org.density.action.search.StreamSearchAction;
import org.density.action.support.PlainActionFuture;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.action.termvectors.MultiTermVectorsAction;
import org.density.action.termvectors.MultiTermVectorsRequest;
import org.density.action.termvectors.MultiTermVectorsRequestBuilder;
import org.density.action.termvectors.MultiTermVectorsResponse;
import org.density.action.termvectors.TermVectorsAction;
import org.density.action.termvectors.TermVectorsRequest;
import org.density.action.termvectors.TermVectorsRequestBuilder;
import org.density.action.termvectors.TermVectorsResponse;
import org.density.action.update.UpdateAction;
import org.density.action.update.UpdateRequest;
import org.density.action.update.UpdateRequestBuilder;
import org.density.action.update.UpdateResponse;
import org.density.cluster.metadata.IndexMetadata.APIBlock;
import org.density.common.Nullable;
import org.density.common.action.ActionFuture;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.ThreadContext;
import org.density.common.util.concurrent.ThreadContextAccess;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.common.bytes.BytesReference;
import org.density.core.tasks.TaskId;
import org.density.core.xcontent.MediaType;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.AdminClient;
import org.density.transport.client.Client;
import org.density.transport.client.ClusterAdminClient;
import org.density.transport.client.FilterClient;
import org.density.transport.client.IndicesAdminClient;
import org.density.transport.client.DensityClient;

import java.util.Map;

/**
 * Base client used to create concrete client implementations
 *
 * @density.internal
 */
public abstract class AbstractClient implements Client {

    protected final Logger logger;

    protected final Settings settings;
    private final ThreadPool threadPool;
    private final Admin admin;

    public AbstractClient(Settings settings, ThreadPool threadPool) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.admin = new Admin(this);
        this.logger = LogManager.getLogger(this.getClass());
    }

    @Override
    public final Settings settings() {
        return this.settings;
    }

    @Override
    public final ThreadPool threadPool() {
        return this.threadPool;
    }

    @Override
    public final AdminClient admin() {
        return admin;
    }

    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
        ActionType<Response> action,
        Request request
    ) {
        PlainActionFuture<Response> actionFuture = PlainActionFuture.newFuture();
        execute(action, request, actionFuture);
        return actionFuture;
    }

    /**
     * This is the single execution point of *all* clients.
     */
    @Override
    public final <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        doExecute(action, request, listener);
    }

    protected abstract <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    );

    @Override
    public ActionFuture<IndexResponse> index(final IndexRequest request) {
        return execute(IndexAction.INSTANCE, request);
    }

    @Override
    public void index(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        execute(IndexAction.INSTANCE, request, listener);
    }

    @Override
    public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(this, IndexAction.INSTANCE, null);
    }

    @Override
    public IndexRequestBuilder prepareIndex(String index) {
        return new IndexRequestBuilder(this, IndexAction.INSTANCE, index);
    }

    @Override
    public ActionFuture<UpdateResponse> update(final UpdateRequest request) {
        return execute(UpdateAction.INSTANCE, request);
    }

    @Override
    public void update(final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        execute(UpdateAction.INSTANCE, request, listener);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate() {
        return new UpdateRequestBuilder(this, UpdateAction.INSTANCE, null, null);
    }

    @Override
    public UpdateRequestBuilder prepareUpdate(String index, String id) {
        return new UpdateRequestBuilder(this, UpdateAction.INSTANCE, index, id);
    }

    @Override
    public ActionFuture<DeleteResponse> delete(final DeleteRequest request) {
        return execute(DeleteAction.INSTANCE, request);
    }

    @Override
    public void delete(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        execute(DeleteAction.INSTANCE, request, listener);
    }

    @Override
    public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(this, DeleteAction.INSTANCE, null);
    }

    @Override
    public DeleteRequestBuilder prepareDelete(String index, String id) {
        return prepareDelete().setIndex(index).setId(id);
    }

    @Override
    public ActionFuture<BulkResponse> bulk(final BulkRequest request) {
        return execute(BulkAction.INSTANCE, request);
    }

    @Override
    public void bulk(final BulkRequest request, final ActionListener<BulkResponse> listener) {
        execute(BulkAction.INSTANCE, request, listener);
    }

    @Override
    public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(this, BulkAction.INSTANCE);
    }

    @Override
    public BulkRequestBuilder prepareBulk(@Nullable String globalIndex) {
        return new BulkRequestBuilder(this, BulkAction.INSTANCE, globalIndex);
    }

    @Override
    public ActionFuture<GetResponse> get(final GetRequest request) {
        return execute(GetAction.INSTANCE, request);
    }

    @Override
    public void get(final GetRequest request, final ActionListener<GetResponse> listener) {
        execute(GetAction.INSTANCE, request, listener);
    }

    @Override
    public GetRequestBuilder prepareGet() {
        return new GetRequestBuilder(this, GetAction.INSTANCE, null);
    }

    @Override
    public GetRequestBuilder prepareGet(String index, String id) {
        return prepareGet().setIndex(index).setId(id);
    }

    @Override
    public ActionFuture<MultiGetResponse> multiGet(final MultiGetRequest request) {
        return execute(MultiGetAction.INSTANCE, request);
    }

    @Override
    public void multiGet(final MultiGetRequest request, final ActionListener<MultiGetResponse> listener) {
        execute(MultiGetAction.INSTANCE, request, listener);
    }

    @Override
    public MultiGetRequestBuilder prepareMultiGet() {
        return new MultiGetRequestBuilder(this, MultiGetAction.INSTANCE);
    }

    @Override
    public ActionFuture<SearchResponse> search(final SearchRequest request) {
        return execute(SearchAction.INSTANCE, request);
    }

    @Override
    public void search(final SearchRequest request, final ActionListener<SearchResponse> listener) {
        execute(SearchAction.INSTANCE, request, listener);
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(this, SearchAction.INSTANCE).setIndices(indices);
    }

    @Override
    public SearchRequestBuilder prepareStreamSearch(String... indices) {
        return new SearchRequestBuilder(this, StreamSearchAction.INSTANCE).setIndices(indices);
    }

    @Override
    public ActionFuture<SearchResponse> searchScroll(final SearchScrollRequest request) {
        return execute(SearchScrollAction.INSTANCE, request);
    }

    @Override
    public void searchScroll(final SearchScrollRequest request, final ActionListener<SearchResponse> listener) {
        execute(SearchScrollAction.INSTANCE, request, listener);
    }

    @Override
    public SearchScrollRequestBuilder prepareSearchScroll(String scrollId) {
        return new SearchScrollRequestBuilder(this, SearchScrollAction.INSTANCE, scrollId);
    }

    @Override
    public void createPit(final CreatePitRequest createPITRequest, final ActionListener<CreatePitResponse> listener) {
        execute(CreatePitAction.INSTANCE, createPITRequest, listener);
    }

    @Override
    public void deletePits(final DeletePitRequest deletePITRequest, final ActionListener<DeletePitResponse> listener) {
        execute(DeletePitAction.INSTANCE, deletePITRequest, listener);
    }

    @Override
    public void getAllPits(final GetAllPitNodesRequest getAllPitNodesRequest, final ActionListener<GetAllPitNodesResponse> listener) {
        execute(GetAllPitsAction.INSTANCE, getAllPitNodesRequest, listener);
    }

    @Override
    public void pitSegments(final PitSegmentsRequest request, final ActionListener<IndicesSegmentResponse> listener) {
        execute(PitSegmentsAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<MultiSearchResponse> multiSearch(MultiSearchRequest request) {
        return execute(MultiSearchAction.INSTANCE, request);
    }

    @Override
    public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
        execute(MultiSearchAction.INSTANCE, request, listener);
    }

    @Override
    public MultiSearchRequestBuilder prepareMultiSearch() {
        return new MultiSearchRequestBuilder(this, MultiSearchAction.INSTANCE);
    }

    @Override
    public ActionFuture<TermVectorsResponse> termVectors(final TermVectorsRequest request) {
        return execute(TermVectorsAction.INSTANCE, request);
    }

    @Override
    public void termVectors(final TermVectorsRequest request, final ActionListener<TermVectorsResponse> listener) {
        execute(TermVectorsAction.INSTANCE, request, listener);
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors() {
        return new TermVectorsRequestBuilder(this, TermVectorsAction.INSTANCE);
    }

    @Override
    public TermVectorsRequestBuilder prepareTermVectors(String index, String id) {
        return new TermVectorsRequestBuilder(this, TermVectorsAction.INSTANCE, index, id);
    }

    @Override
    public ActionFuture<MultiTermVectorsResponse> multiTermVectors(final MultiTermVectorsRequest request) {
        return execute(MultiTermVectorsAction.INSTANCE, request);
    }

    @Override
    public void multiTermVectors(final MultiTermVectorsRequest request, final ActionListener<MultiTermVectorsResponse> listener) {
        execute(MultiTermVectorsAction.INSTANCE, request, listener);
    }

    @Override
    public MultiTermVectorsRequestBuilder prepareMultiTermVectors() {
        return new MultiTermVectorsRequestBuilder(this, MultiTermVectorsAction.INSTANCE);
    }

    @Override
    public ExplainRequestBuilder prepareExplain(String index, String id) {
        return new ExplainRequestBuilder(this, ExplainAction.INSTANCE, index, id);
    }

    @Override
    public ActionFuture<ExplainResponse> explain(ExplainRequest request) {
        return execute(ExplainAction.INSTANCE, request);
    }

    @Override
    public void explain(ExplainRequest request, ActionListener<ExplainResponse> listener) {
        execute(ExplainAction.INSTANCE, request, listener);
    }

    @Override
    public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
        execute(ClearScrollAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<ClearScrollResponse> clearScroll(ClearScrollRequest request) {
        return execute(ClearScrollAction.INSTANCE, request);
    }

    @Override
    public ClearScrollRequestBuilder prepareClearScroll() {
        return new ClearScrollRequestBuilder(this, ClearScrollAction.INSTANCE);
    }

    @Override
    public void fieldCaps(FieldCapabilitiesRequest request, ActionListener<FieldCapabilitiesResponse> listener) {
        execute(FieldCapabilitiesAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<FieldCapabilitiesResponse> fieldCaps(FieldCapabilitiesRequest request) {
        return execute(FieldCapabilitiesAction.INSTANCE, request);
    }

    @Override
    public FieldCapabilitiesRequestBuilder prepareFieldCaps(String... indices) {
        return new FieldCapabilitiesRequestBuilder(this, FieldCapabilitiesAction.INSTANCE, indices);
    }

    @Override
    public void searchView(final SearchViewAction.Request request, final ActionListener<SearchResponse> listener) {
        execute(SearchViewAction.INSTANCE, request);
    }

    @Override
    public ActionFuture<SearchResponse> searchView(final SearchViewAction.Request request) {
        return execute(SearchViewAction.INSTANCE, request);
    }

    @Override
    public void listViewNames(final ListViewNamesAction.Request request, ActionListener<ListViewNamesAction.Response> listener) {
        execute(ListViewNamesAction.INSTANCE, request, listener);
    }

    @Override
    public ActionFuture<ListViewNamesAction.Response> listViewNames(final ListViewNamesAction.Request request) {
        return execute(ListViewNamesAction.INSTANCE, request);
    }

    static class Admin implements AdminClient {

        private final ClusterAdmin clusterAdmin;
        private final IndicesAdmin indicesAdmin;

        Admin(DensityClient client) {
            this.clusterAdmin = new ClusterAdmin(client);
            this.indicesAdmin = new IndicesAdmin(client);
        }

        @Override
        public ClusterAdminClient cluster() {
            return clusterAdmin;
        }

        @Override
        public IndicesAdminClient indices() {
            return indicesAdmin;
        }
    }

    static class ClusterAdmin implements ClusterAdminClient {

        private final DensityClient client;

        ClusterAdmin(DensityClient client) {
            this.client = client;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
            ActionType<Response> action,
            Request request
        ) {
            return client.execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void execute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            client.execute(action, request, listener);
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
        }

        @Override
        public ActionFuture<ClusterHealthResponse> health(final ClusterHealthRequest request) {
            return execute(ClusterHealthAction.INSTANCE, request);
        }

        @Override
        public void health(final ClusterHealthRequest request, final ActionListener<ClusterHealthResponse> listener) {
            execute(ClusterHealthAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterHealthRequestBuilder prepareHealth(String... indices) {
            return new ClusterHealthRequestBuilder(this, ClusterHealthAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<ClusterStateResponse> state(final ClusterStateRequest request) {
            return execute(ClusterStateAction.INSTANCE, request);
        }

        @Override
        public void state(final ClusterStateRequest request, final ActionListener<ClusterStateResponse> listener) {
            execute(ClusterStateAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterStateRequestBuilder prepareState() {
            return new ClusterStateRequestBuilder(this, ClusterStateAction.INSTANCE);
        }

        @Override
        public ActionFuture<ClusterRerouteResponse> reroute(final ClusterRerouteRequest request) {
            return execute(ClusterRerouteAction.INSTANCE, request);
        }

        @Override
        public void reroute(final ClusterRerouteRequest request, final ActionListener<ClusterRerouteResponse> listener) {
            execute(ClusterRerouteAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterRerouteRequestBuilder prepareReroute() {
            return new ClusterRerouteRequestBuilder(this, ClusterRerouteAction.INSTANCE);
        }

        @Override
        public ActionFuture<ClusterUpdateSettingsResponse> updateSettings(final ClusterUpdateSettingsRequest request) {
            return execute(ClusterUpdateSettingsAction.INSTANCE, request);
        }

        @Override
        public void updateSettings(
            final ClusterUpdateSettingsRequest request,
            final ActionListener<ClusterUpdateSettingsResponse> listener
        ) {
            execute(ClusterUpdateSettingsAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterUpdateSettingsRequestBuilder prepareUpdateSettings() {
            return new ClusterUpdateSettingsRequestBuilder(this, ClusterUpdateSettingsAction.INSTANCE);
        }

        @Override
        public NodesReloadSecureSettingsRequestBuilder prepareReloadSecureSettings() {
            return new NodesReloadSecureSettingsRequestBuilder(this, NodesReloadSecureSettingsAction.INSTANCE);
        }

        @Override
        public ActionFuture<NodesInfoResponse> nodesInfo(final NodesInfoRequest request) {
            return execute(NodesInfoAction.INSTANCE, request);
        }

        @Override
        public void nodesInfo(final NodesInfoRequest request, final ActionListener<NodesInfoResponse> listener) {
            execute(NodesInfoAction.INSTANCE, request, listener);
        }

        @Override
        public NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds) {
            return new NodesInfoRequestBuilder(this, NodesInfoAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public ActionFuture<NodesStatsResponse> nodesStats(final NodesStatsRequest request) {
            return execute(NodesStatsAction.INSTANCE, request);
        }

        @Override
        public void nodesStats(final NodesStatsRequest request, final ActionListener<NodesStatsResponse> listener) {
            execute(NodesStatsAction.INSTANCE, request, listener);
        }

        @Override
        public NodesStatsRequestBuilder prepareNodesStats(String... nodesIds) {
            return new NodesStatsRequestBuilder(this, NodesStatsAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public void wlmStats(final WlmStatsRequest request, final ActionListener<WlmStatsResponse> listener) {
            execute(WlmStatsAction.INSTANCE, request, listener);
        }

        @Override
        public void remoteStoreStats(final RemoteStoreStatsRequest request, final ActionListener<RemoteStoreStatsResponse> listener) {
            execute(RemoteStoreStatsAction.INSTANCE, request, listener);
        }

        @Override
        public RemoteStoreStatsRequestBuilder prepareRemoteStoreStats(String index, String shardId) {
            RemoteStoreStatsRequestBuilder remoteStoreStatsRequestBuilder = new RemoteStoreStatsRequestBuilder(
                this,
                RemoteStoreStatsAction.INSTANCE
            ).setIndices(index);
            if (shardId != null) {
                remoteStoreStatsRequestBuilder.setShards(shardId);
            }
            return remoteStoreStatsRequestBuilder;
        }

        @Override
        public void remoteStoreMetadata(
            final RemoteStoreMetadataRequest request,
            final ActionListener<RemoteStoreMetadataResponse> listener
        ) {
            execute(RemoteStoreMetadataAction.INSTANCE, request, listener);
        }

        @Override
        public RemoteStoreMetadataRequestBuilder prepareRemoteStoreMetadata(String index, String shardId) {
            RemoteStoreMetadataRequestBuilder builder = new RemoteStoreMetadataRequestBuilder(this, RemoteStoreMetadataAction.INSTANCE)
                .setIndices(index);
            if (shardId != null) {
                builder.setShards(shardId);
            }
            return builder;
        }

        @Override
        public ActionFuture<NodesUsageResponse> nodesUsage(final NodesUsageRequest request) {
            return execute(NodesUsageAction.INSTANCE, request);
        }

        @Override
        public void nodesUsage(final NodesUsageRequest request, final ActionListener<NodesUsageResponse> listener) {
            execute(NodesUsageAction.INSTANCE, request, listener);
        }

        @Override
        public NodesUsageRequestBuilder prepareNodesUsage(String... nodesIds) {
            return new NodesUsageRequestBuilder(this, NodesUsageAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public ActionFuture<ClusterStatsResponse> clusterStats(ClusterStatsRequest request) {
            return execute(ClusterStatsAction.INSTANCE, request);
        }

        @Override
        public void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener) {
            execute(ClusterStatsAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterStatsRequestBuilder prepareClusterStats() {
            return new ClusterStatsRequestBuilder(this, ClusterStatsAction.INSTANCE);
        }

        @Override
        public ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request) {
            return execute(NodesHotThreadsAction.INSTANCE, request);
        }

        @Override
        public void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener) {
            execute(NodesHotThreadsAction.INSTANCE, request, listener);
        }

        @Override
        public NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds) {
            return new NodesHotThreadsRequestBuilder(this, NodesHotThreadsAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public ActionFuture<ListTasksResponse> listTasks(final ListTasksRequest request) {
            return execute(ListTasksAction.INSTANCE, request);
        }

        @Override
        public void listTasks(final ListTasksRequest request, final ActionListener<ListTasksResponse> listener) {
            execute(ListTasksAction.INSTANCE, request, listener);
        }

        @Override
        public ListTasksRequestBuilder prepareListTasks(String... nodesIds) {
            return new ListTasksRequestBuilder(this, ListTasksAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public ActionFuture<GetTaskResponse> getTask(final GetTaskRequest request) {
            return execute(GetTaskAction.INSTANCE, request);
        }

        @Override
        public void getTask(final GetTaskRequest request, final ActionListener<GetTaskResponse> listener) {
            execute(GetTaskAction.INSTANCE, request, listener);
        }

        @Override
        public GetTaskRequestBuilder prepareGetTask(String taskId) {
            return prepareGetTask(new TaskId(taskId));
        }

        @Override
        public GetTaskRequestBuilder prepareGetTask(TaskId taskId) {
            return new GetTaskRequestBuilder(this, GetTaskAction.INSTANCE).setTaskId(taskId);
        }

        @Override
        public ActionFuture<CancelTasksResponse> cancelTasks(CancelTasksRequest request) {
            return execute(CancelTasksAction.INSTANCE, request);
        }

        @Override
        public void cancelTasks(CancelTasksRequest request, ActionListener<CancelTasksResponse> listener) {
            execute(CancelTasksAction.INSTANCE, request, listener);
        }

        @Override
        public CancelTasksRequestBuilder prepareCancelTasks(String... nodesIds) {
            return new CancelTasksRequestBuilder(this, CancelTasksAction.INSTANCE).setNodesIds(nodesIds);
        }

        @Override
        public ActionFuture<ClusterSearchShardsResponse> searchShards(final ClusterSearchShardsRequest request) {
            return execute(ClusterSearchShardsAction.INSTANCE, request);
        }

        @Override
        public void searchShards(final ClusterSearchShardsRequest request, final ActionListener<ClusterSearchShardsResponse> listener) {
            execute(ClusterSearchShardsAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterSearchShardsRequestBuilder prepareSearchShards() {
            return new ClusterSearchShardsRequestBuilder(this, ClusterSearchShardsAction.INSTANCE);
        }

        @Override
        public ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices) {
            return new ClusterSearchShardsRequestBuilder(this, ClusterSearchShardsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public PendingClusterTasksRequestBuilder preparePendingClusterTasks() {
            return new PendingClusterTasksRequestBuilder(this, PendingClusterTasksAction.INSTANCE);
        }

        @Override
        public ActionFuture<PendingClusterTasksResponse> pendingClusterTasks(PendingClusterTasksRequest request) {
            return execute(PendingClusterTasksAction.INSTANCE, request);
        }

        @Override
        public void pendingClusterTasks(PendingClusterTasksRequest request, ActionListener<PendingClusterTasksResponse> listener) {
            execute(PendingClusterTasksAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putRepository(PutRepositoryRequest request) {
            return execute(PutRepositoryAction.INSTANCE, request);
        }

        @Override
        public void putRepository(PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(PutRepositoryAction.INSTANCE, request, listener);
        }

        @Override
        public PutRepositoryRequestBuilder preparePutRepository(String name) {
            return new PutRepositoryRequestBuilder(this, PutRepositoryAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request) {
            return execute(CreateSnapshotAction.INSTANCE, request);
        }

        @Override
        public void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener) {
            execute(CreateSnapshotAction.INSTANCE, request, listener);
        }

        @Override
        public CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name) {
            return new CreateSnapshotRequestBuilder(this, CreateSnapshotAction.INSTANCE, repository, name);
        }

        @Override
        public CloneSnapshotRequestBuilder prepareCloneSnapshot(String repository, String source, String target) {
            return new CloneSnapshotRequestBuilder(this, CloneSnapshotAction.INSTANCE, repository, source, target);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> cloneSnapshot(CloneSnapshotRequest request) {
            return execute(CloneSnapshotAction.INSTANCE, request);
        }

        @Override
        public void cloneSnapshot(CloneSnapshotRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(CloneSnapshotAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<GetSnapshotsResponse> getSnapshots(GetSnapshotsRequest request) {
            return execute(GetSnapshotsAction.INSTANCE, request);
        }

        @Override
        public void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener) {
            execute(GetSnapshotsAction.INSTANCE, request, listener);
        }

        @Override
        public GetSnapshotsRequestBuilder prepareGetSnapshots(String repository) {
            return new GetSnapshotsRequestBuilder(this, GetSnapshotsAction.INSTANCE, repository);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> deleteSnapshot(DeleteSnapshotRequest request) {
            return execute(DeleteSnapshotAction.INSTANCE, request);
        }

        @Override
        public void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteSnapshotAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String... names) {
            return new DeleteSnapshotRequestBuilder(this, DeleteSnapshotAction.INSTANCE, repository, names);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> deleteRepository(DeleteRepositoryRequest request) {
            return execute(DeleteRepositoryAction.INSTANCE, request);
        }

        @Override
        public void deleteRepository(DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteRepositoryAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteRepositoryRequestBuilder prepareDeleteRepository(String name) {
            return new DeleteRepositoryRequestBuilder(this, DeleteRepositoryAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<VerifyRepositoryResponse> verifyRepository(VerifyRepositoryRequest request) {
            return execute(VerifyRepositoryAction.INSTANCE, request);
        }

        @Override
        public void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener) {
            execute(VerifyRepositoryAction.INSTANCE, request, listener);
        }

        @Override
        public VerifyRepositoryRequestBuilder prepareVerifyRepository(String name) {
            return new VerifyRepositoryRequestBuilder(this, VerifyRepositoryAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<GetRepositoriesResponse> getRepositories(GetRepositoriesRequest request) {
            return execute(GetRepositoriesAction.INSTANCE, request);
        }

        @Override
        public void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener) {
            execute(GetRepositoriesAction.INSTANCE, request, listener);
        }

        @Override
        public GetRepositoriesRequestBuilder prepareGetRepositories(String... name) {
            return new GetRepositoriesRequestBuilder(this, GetRepositoriesAction.INSTANCE, name);
        }

        @Override
        public CleanupRepositoryRequestBuilder prepareCleanupRepository(String repository) {
            return new CleanupRepositoryRequestBuilder(this, CleanupRepositoryAction.INSTANCE, repository);
        }

        @Override
        public ActionFuture<CleanupRepositoryResponse> cleanupRepository(CleanupRepositoryRequest request) {
            return execute(CleanupRepositoryAction.INSTANCE, request);
        }

        @Override
        public void cleanupRepository(CleanupRepositoryRequest request, ActionListener<CleanupRepositoryResponse> listener) {
            execute(CleanupRepositoryAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request) {
            return execute(RestoreSnapshotAction.INSTANCE, request);
        }

        @Override
        public void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener) {
            execute(RestoreSnapshotAction.INSTANCE, request, listener);
        }

        @Override
        public void restoreRemoteStore(RestoreRemoteStoreRequest request, ActionListener<RestoreRemoteStoreResponse> listener) {
            execute(RestoreRemoteStoreAction.INSTANCE, request, listener);
        }

        @Override
        public RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot) {
            return new RestoreSnapshotRequestBuilder(this, RestoreSnapshotAction.INSTANCE, repository, snapshot);
        }

        @Override
        public ActionFuture<SnapshotsStatusResponse> snapshotsStatus(SnapshotsStatusRequest request) {
            return execute(SnapshotsStatusAction.INSTANCE, request);
        }

        @Override
        public void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener) {
            execute(SnapshotsStatusAction.INSTANCE, request, listener);
        }

        @Override
        public SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository) {
            return new SnapshotsStatusRequestBuilder(this, SnapshotsStatusAction.INSTANCE, repository);
        }

        @Override
        public SnapshotsStatusRequestBuilder prepareSnapshotStatus() {
            return new SnapshotsStatusRequestBuilder(this, SnapshotsStatusAction.INSTANCE);
        }

        @Override
        public void putPipeline(PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(PutPipelineAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putPipeline(PutPipelineRequest request) {
            return execute(PutPipelineAction.INSTANCE, request);
        }

        @Override
        public PutPipelineRequestBuilder preparePutPipeline(String id, BytesReference source, MediaType mediaType) {
            return new PutPipelineRequestBuilder(this, PutPipelineAction.INSTANCE, id, source, mediaType);
        }

        @Override
        public void deletePipeline(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(DeletePipelineAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> deletePipeline(DeletePipelineRequest request) {
            return execute(DeletePipelineAction.INSTANCE, request);
        }

        @Override
        public DeletePipelineRequestBuilder prepareDeletePipeline() {
            return new DeletePipelineRequestBuilder(this, DeletePipelineAction.INSTANCE);
        }

        @Override
        public DeletePipelineRequestBuilder prepareDeletePipeline(String id) {
            return new DeletePipelineRequestBuilder(this, DeletePipelineAction.INSTANCE, id);
        }

        @Override
        public void getPipeline(GetPipelineRequest request, ActionListener<GetPipelineResponse> listener) {
            execute(GetPipelineAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<GetPipelineResponse> getPipeline(GetPipelineRequest request) {
            return execute(GetPipelineAction.INSTANCE, request);
        }

        @Override
        public GetPipelineRequestBuilder prepareGetPipeline(String... ids) {
            return new GetPipelineRequestBuilder(this, GetPipelineAction.INSTANCE, ids);
        }

        @Override
        public void simulatePipeline(SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener) {
            execute(SimulatePipelineAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<SimulatePipelineResponse> simulatePipeline(SimulatePipelineRequest request) {
            return execute(SimulatePipelineAction.INSTANCE, request);
        }

        @Override
        public SimulatePipelineRequestBuilder prepareSimulatePipeline(BytesReference source, MediaType mediaType) {
            return new SimulatePipelineRequestBuilder(this, SimulatePipelineAction.INSTANCE, source, mediaType);
        }

        @Override
        public void allocationExplain(ClusterAllocationExplainRequest request, ActionListener<ClusterAllocationExplainResponse> listener) {
            execute(ClusterAllocationExplainAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<ClusterAllocationExplainResponse> allocationExplain(ClusterAllocationExplainRequest request) {
            return execute(ClusterAllocationExplainAction.INSTANCE, request);
        }

        @Override
        public ClusterAllocationExplainRequestBuilder prepareAllocationExplain() {
            return new ClusterAllocationExplainRequestBuilder(this, ClusterAllocationExplainAction.INSTANCE);
        }

        @Override
        public ActionFuture<GetStoredScriptResponse> getStoredScript(final GetStoredScriptRequest request) {
            return execute(GetStoredScriptAction.INSTANCE, request);
        }

        @Override
        public void getStoredScript(final GetStoredScriptRequest request, final ActionListener<GetStoredScriptResponse> listener) {
            execute(GetStoredScriptAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<ListDanglingIndicesResponse> listDanglingIndices(ListDanglingIndicesRequest request) {
            return execute(ListDanglingIndicesAction.INSTANCE, request);
        }

        @Override
        public void listDanglingIndices(ListDanglingIndicesRequest request, ActionListener<ListDanglingIndicesResponse> listener) {
            execute(ListDanglingIndicesAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> importDanglingIndex(ImportDanglingIndexRequest request) {
            return execute(ImportDanglingIndexAction.INSTANCE, request);
        }

        @Override
        public void importDanglingIndex(ImportDanglingIndexRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(ImportDanglingIndexAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> deleteDanglingIndex(DeleteDanglingIndexRequest request) {
            return execute(DeleteDanglingIndexAction.INSTANCE, request);
        }

        @Override
        public ActionFuture<ClusterPutWeightedRoutingResponse> putWeightedRouting(ClusterPutWeightedRoutingRequest request) {
            return execute(ClusterAddWeightedRoutingAction.INSTANCE, request);
        }

        @Override
        public void putWeightedRouting(
            ClusterPutWeightedRoutingRequest request,
            ActionListener<ClusterPutWeightedRoutingResponse> listener
        ) {
            execute(ClusterAddWeightedRoutingAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterPutWeightedRoutingRequestBuilder prepareWeightedRouting() {
            return new ClusterPutWeightedRoutingRequestBuilder(this, ClusterAddWeightedRoutingAction.INSTANCE);
        }

        @Override
        public ActionFuture<ClusterGetWeightedRoutingResponse> getWeightedRouting(ClusterGetWeightedRoutingRequest request) {
            return execute(ClusterGetWeightedRoutingAction.INSTANCE, request);
        }

        @Override
        public void getWeightedRouting(
            ClusterGetWeightedRoutingRequest request,
            ActionListener<ClusterGetWeightedRoutingResponse> listener
        ) {
            execute(ClusterGetWeightedRoutingAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterGetWeightedRoutingRequestBuilder prepareGetWeightedRouting() {
            return new ClusterGetWeightedRoutingRequestBuilder(this, ClusterGetWeightedRoutingAction.INSTANCE);
        }

        @Override
        public ActionFuture<ClusterDeleteWeightedRoutingResponse> deleteWeightedRouting(ClusterDeleteWeightedRoutingRequest request) {
            return execute(ClusterDeleteWeightedRoutingAction.INSTANCE, request);
        }

        @Override
        public void deleteWeightedRouting(
            ClusterDeleteWeightedRoutingRequest request,
            ActionListener<ClusterDeleteWeightedRoutingResponse> listener
        ) {
            execute(ClusterDeleteWeightedRoutingAction.INSTANCE, request, listener);
        }

        @Override
        public ClusterDeleteWeightedRoutingRequestBuilder prepareDeleteWeightedRouting() {
            return new ClusterDeleteWeightedRoutingRequestBuilder(this, ClusterDeleteWeightedRoutingAction.INSTANCE);
        }

        @Override
        public void deleteDanglingIndex(DeleteDanglingIndexRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteDanglingIndexAction.INSTANCE, request, listener);
        }

        @Override
        public GetStoredScriptRequestBuilder prepareGetStoredScript() {
            return new GetStoredScriptRequestBuilder(this, GetStoredScriptAction.INSTANCE);
        }

        @Override
        public GetStoredScriptRequestBuilder prepareGetStoredScript(String id) {
            return prepareGetStoredScript().setId(id);
        }

        @Override
        public PutStoredScriptRequestBuilder preparePutStoredScript() {
            return new PutStoredScriptRequestBuilder(this, PutStoredScriptAction.INSTANCE);
        }

        @Override
        public void putStoredScript(final PutStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(PutStoredScriptAction.INSTANCE, request, listener);

        }

        @Override
        public ActionFuture<AcknowledgedResponse> putStoredScript(final PutStoredScriptRequest request) {
            return execute(PutStoredScriptAction.INSTANCE, request);
        }

        @Override
        public void deleteStoredScript(DeleteStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteStoredScriptAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> deleteStoredScript(DeleteStoredScriptRequest request) {
            return execute(DeleteStoredScriptAction.INSTANCE, request);
        }

        @Override
        public DeleteStoredScriptRequestBuilder prepareDeleteStoredScript() {
            return new DeleteStoredScriptRequestBuilder(client, DeleteStoredScriptAction.INSTANCE);
        }

        @Override
        public DeleteStoredScriptRequestBuilder prepareDeleteStoredScript(String id) {
            return prepareDeleteStoredScript().setId(id);
        }

        @Override
        public ActionFuture<DecommissionResponse> decommission(DecommissionRequest request) {
            return execute(DecommissionAction.INSTANCE, request);
        }

        @Override
        public void decommission(DecommissionRequest request, ActionListener<DecommissionResponse> listener) {
            execute(DecommissionAction.INSTANCE, request, listener);
        }

        @Override
        public DecommissionRequestBuilder prepareDecommission(DecommissionRequest request) {
            return new DecommissionRequestBuilder(this, DecommissionAction.INSTANCE, request);
        }

        @Override
        public ActionFuture<GetDecommissionStateResponse> getDecommissionState(GetDecommissionStateRequest request) {
            return execute(GetDecommissionStateAction.INSTANCE, request);
        }

        @Override
        public void getDecommissionState(GetDecommissionStateRequest request, ActionListener<GetDecommissionStateResponse> listener) {
            execute(GetDecommissionStateAction.INSTANCE, request, listener);
        }

        @Override
        public GetDecommissionStateRequestBuilder prepareGetDecommissionState() {
            return new GetDecommissionStateRequestBuilder(this, GetDecommissionStateAction.INSTANCE);
        }

        @Override
        public ActionFuture<DeleteDecommissionStateResponse> deleteDecommissionState(DeleteDecommissionStateRequest request) {
            return execute(DeleteDecommissionStateAction.INSTANCE, request);
        }

        @Override
        public void deleteDecommissionState(
            DeleteDecommissionStateRequest request,
            ActionListener<DeleteDecommissionStateResponse> listener
        ) {
            execute(DeleteDecommissionStateAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteDecommissionStateRequestBuilder prepareDeleteDecommissionRequest() {
            return new DeleteDecommissionStateRequestBuilder(this, DeleteDecommissionStateAction.INSTANCE);
        }

        @Override
        public void putSearchPipeline(PutSearchPipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(PutSearchPipelineAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putSearchPipeline(PutSearchPipelineRequest request) {
            return execute(PutSearchPipelineAction.INSTANCE, request);
        }

        @Override
        public void getSearchPipeline(GetSearchPipelineRequest request, ActionListener<GetSearchPipelineResponse> listener) {
            execute(GetSearchPipelineAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<GetSearchPipelineResponse> getSearchPipeline(GetSearchPipelineRequest request) {
            return execute(GetSearchPipelineAction.INSTANCE, request);
        }

        @Override
        public void deleteSearchPipeline(DeleteSearchPipelineRequest request, ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteSearchPipelineAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> deleteSearchPipeline(DeleteSearchPipelineRequest request) {
            return execute(DeleteSearchPipelineAction.INSTANCE, request);
        }
    }

    static class IndicesAdmin implements IndicesAdminClient {

        private final DensityClient client;

        IndicesAdmin(DensityClient client) {
            this.client = client;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> ActionFuture<Response> execute(
            ActionType<Response> action,
            Request request
        ) {
            return client.execute(action, request);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void execute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            client.execute(action, request, listener);
        }

        @Override
        public ThreadPool threadPool() {
            return client.threadPool();
        }

        @Override
        public ActionFuture<IndicesExistsResponse> exists(final IndicesExistsRequest request) {
            return execute(IndicesExistsAction.INSTANCE, request);
        }

        @Override
        public void exists(final IndicesExistsRequest request, final ActionListener<IndicesExistsResponse> listener) {
            execute(IndicesExistsAction.INSTANCE, request, listener);
        }

        @Override
        public IndicesExistsRequestBuilder prepareExists(String... indices) {
            return new IndicesExistsRequestBuilder(this, IndicesExistsAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> aliases(final IndicesAliasesRequest request) {
            return execute(IndicesAliasesAction.INSTANCE, request);
        }

        @Override
        public void aliases(final IndicesAliasesRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(IndicesAliasesAction.INSTANCE, request, listener);
        }

        @Override
        public IndicesAliasesRequestBuilder prepareAliases() {
            return new IndicesAliasesRequestBuilder(this, IndicesAliasesAction.INSTANCE);
        }

        @Override
        public ActionFuture<GetAliasesResponse> getAliases(GetAliasesRequest request) {
            return execute(GetAliasesAction.INSTANCE, request);
        }

        @Override
        public void getAliases(GetAliasesRequest request, ActionListener<GetAliasesResponse> listener) {
            execute(GetAliasesAction.INSTANCE, request, listener);
        }

        @Override
        public GetAliasesRequestBuilder prepareGetAliases(String... aliases) {
            return new GetAliasesRequestBuilder(this, GetAliasesAction.INSTANCE, aliases);
        }

        @Override
        public ActionFuture<ClearIndicesCacheResponse> clearCache(final ClearIndicesCacheRequest request) {
            return execute(ClearIndicesCacheAction.INSTANCE, request);
        }

        @Override
        public ActionFuture<GetIndexResponse> getIndex(GetIndexRequest request) {
            return execute(GetIndexAction.INSTANCE, request);
        }

        @Override
        public void getIndex(GetIndexRequest request, ActionListener<GetIndexResponse> listener) {
            execute(GetIndexAction.INSTANCE, request, listener);
        }

        @Override
        public GetIndexRequestBuilder prepareGetIndex() {
            return new GetIndexRequestBuilder(this, GetIndexAction.INSTANCE);
        }

        @Override
        public void clearCache(final ClearIndicesCacheRequest request, final ActionListener<ClearIndicesCacheResponse> listener) {
            execute(ClearIndicesCacheAction.INSTANCE, request, listener);
        }

        @Override
        public ClearIndicesCacheRequestBuilder prepareClearCache(String... indices) {
            return new ClearIndicesCacheRequestBuilder(this, ClearIndicesCacheAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<CreateIndexResponse> create(final CreateIndexRequest request) {
            return execute(CreateIndexAction.INSTANCE, request);
        }

        @Override
        public void create(final CreateIndexRequest request, final ActionListener<CreateIndexResponse> listener) {
            execute(CreateIndexAction.INSTANCE, request, listener);
        }

        @Override
        public CreateIndexRequestBuilder prepareCreate(String index) {
            return new CreateIndexRequestBuilder(this, CreateIndexAction.INSTANCE, index);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> delete(final DeleteIndexRequest request) {
            return execute(DeleteIndexAction.INSTANCE, request);
        }

        @Override
        public void delete(final DeleteIndexRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteIndexAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteIndexRequestBuilder prepareDelete(String... indices) {
            return new DeleteIndexRequestBuilder(this, DeleteIndexAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<CloseIndexResponse> close(final CloseIndexRequest request) {
            return execute(CloseIndexAction.INSTANCE, request);
        }

        @Override
        public void close(final CloseIndexRequest request, final ActionListener<CloseIndexResponse> listener) {
            execute(CloseIndexAction.INSTANCE, request, listener);
        }

        @Override
        public CloseIndexRequestBuilder prepareClose(String... indices) {
            return new CloseIndexRequestBuilder(this, CloseIndexAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<OpenIndexResponse> open(final OpenIndexRequest request) {
            return execute(OpenIndexAction.INSTANCE, request);
        }

        @Override
        public void open(final OpenIndexRequest request, final ActionListener<OpenIndexResponse> listener) {
            execute(OpenIndexAction.INSTANCE, request, listener);
        }

        @Override
        public AddIndexBlockRequestBuilder prepareAddBlock(APIBlock block, String... indices) {
            return new AddIndexBlockRequestBuilder(this, AddIndexBlockAction.INSTANCE, block, indices);
        }

        @Override
        public void addBlock(AddIndexBlockRequest request, ActionListener<AddIndexBlockResponse> listener) {
            execute(AddIndexBlockAction.INSTANCE, request, listener);
        }

        @Override
        public OpenIndexRequestBuilder prepareOpen(String... indices) {
            return new OpenIndexRequestBuilder(this, OpenIndexAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<FlushResponse> flush(final FlushRequest request) {
            return execute(FlushAction.INSTANCE, request);
        }

        @Override
        public void flush(final FlushRequest request, final ActionListener<FlushResponse> listener) {
            execute(FlushAction.INSTANCE, request, listener);
        }

        @Override
        public FlushRequestBuilder prepareFlush(String... indices) {
            return new FlushRequestBuilder(this, FlushAction.INSTANCE).setIndices(indices);
        }

        @Override
        public void getMappings(GetMappingsRequest request, ActionListener<GetMappingsResponse> listener) {
            execute(GetMappingsAction.INSTANCE, request, listener);
        }

        @Override
        public void getFieldMappings(GetFieldMappingsRequest request, ActionListener<GetFieldMappingsResponse> listener) {
            execute(GetFieldMappingsAction.INSTANCE, request, listener);
        }

        @Override
        public GetMappingsRequestBuilder prepareGetMappings(String... indices) {
            return new GetMappingsRequestBuilder(this, GetMappingsAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<GetMappingsResponse> getMappings(GetMappingsRequest request) {
            return execute(GetMappingsAction.INSTANCE, request);
        }

        @Override
        public GetFieldMappingsRequestBuilder prepareGetFieldMappings(String... indices) {
            return new GetFieldMappingsRequestBuilder(this, GetFieldMappingsAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<GetFieldMappingsResponse> getFieldMappings(GetFieldMappingsRequest request) {
            return execute(GetFieldMappingsAction.INSTANCE, request);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putMapping(final PutMappingRequest request) {
            return execute(PutMappingAction.INSTANCE, request);
        }

        @Override
        public void putMapping(final PutMappingRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(PutMappingAction.INSTANCE, request, listener);
        }

        @Override
        public PutMappingRequestBuilder preparePutMapping(String... indices) {
            return new PutMappingRequestBuilder(this, PutMappingAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<ForceMergeResponse> forceMerge(final ForceMergeRequest request) {
            return execute(ForceMergeAction.INSTANCE, request);
        }

        @Override
        public void forceMerge(final ForceMergeRequest request, final ActionListener<ForceMergeResponse> listener) {
            execute(ForceMergeAction.INSTANCE, request, listener);
        }

        @Override
        public ForceMergeRequestBuilder prepareForceMerge(String... indices) {
            return new ForceMergeRequestBuilder(this, ForceMergeAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<UpgradeResponse> upgrade(final UpgradeRequest request) {
            return execute(UpgradeAction.INSTANCE, request);
        }

        @Override
        public void upgrade(final UpgradeRequest request, final ActionListener<UpgradeResponse> listener) {
            execute(UpgradeAction.INSTANCE, request, listener);
        }

        @Override
        public UpgradeRequestBuilder prepareUpgrade(String... indices) {
            return new UpgradeRequestBuilder(this, UpgradeAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<UpgradeStatusResponse> upgradeStatus(final UpgradeStatusRequest request) {
            return execute(UpgradeStatusAction.INSTANCE, request);
        }

        @Override
        public void upgradeStatus(final UpgradeStatusRequest request, final ActionListener<UpgradeStatusResponse> listener) {
            execute(UpgradeStatusAction.INSTANCE, request, listener);
        }

        @Override
        public UpgradeStatusRequestBuilder prepareUpgradeStatus(String... indices) {
            return new UpgradeStatusRequestBuilder(this, UpgradeStatusAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<RefreshResponse> refresh(final RefreshRequest request) {
            return execute(RefreshAction.INSTANCE, request);
        }

        @Override
        public void refresh(final RefreshRequest request, final ActionListener<RefreshResponse> listener) {
            execute(RefreshAction.INSTANCE, request, listener);
        }

        @Override
        public RefreshRequestBuilder prepareRefresh(String... indices) {
            return new RefreshRequestBuilder(this, RefreshAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<IndicesStatsResponse> stats(final IndicesStatsRequest request) {
            return execute(IndicesStatsAction.INSTANCE, request);
        }

        @Override
        public void stats(final IndicesStatsRequest request, final ActionListener<IndicesStatsResponse> listener) {
            execute(IndicesStatsAction.INSTANCE, request, listener);
        }

        @Override
        public IndicesStatsRequestBuilder prepareStats(String... indices) {
            return new IndicesStatsRequestBuilder(this, IndicesStatsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<RecoveryResponse> recoveries(final RecoveryRequest request) {
            return execute(RecoveryAction.INSTANCE, request);
        }

        @Override
        public void recoveries(final RecoveryRequest request, final ActionListener<RecoveryResponse> listener) {
            execute(RecoveryAction.INSTANCE, request, listener);
        }

        @Override
        public RecoveryRequestBuilder prepareRecoveries(String... indices) {
            return new RecoveryRequestBuilder(this, RecoveryAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<SegmentReplicationStatsResponse> segmentReplicationStats(final SegmentReplicationStatsRequest request) {
            return execute(SegmentReplicationStatsAction.INSTANCE, request);
        }

        @Override
        public void segmentReplicationStats(
            final SegmentReplicationStatsRequest request,
            final ActionListener<SegmentReplicationStatsResponse> listener
        ) {
            execute(SegmentReplicationStatsAction.INSTANCE, request, listener);
        }

        @Override
        public SegmentReplicationStatsRequestBuilder prepareSegmentReplicationStats(String... indices) {
            return new SegmentReplicationStatsRequestBuilder(this, SegmentReplicationStatsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<IndicesSegmentResponse> segments(final IndicesSegmentsRequest request) {
            return execute(IndicesSegmentsAction.INSTANCE, request);
        }

        @Override
        public void segments(final IndicesSegmentsRequest request, final ActionListener<IndicesSegmentResponse> listener) {
            execute(IndicesSegmentsAction.INSTANCE, request, listener);
        }

        @Override
        public IndicesSegmentsRequestBuilder prepareSegments(String... indices) {
            return new IndicesSegmentsRequestBuilder(this, IndicesSegmentsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<IndicesShardStoresResponse> shardStores(IndicesShardStoresRequest request) {
            return execute(IndicesShardStoresAction.INSTANCE, request);
        }

        @Override
        public void shardStores(IndicesShardStoresRequest request, ActionListener<IndicesShardStoresResponse> listener) {
            execute(IndicesShardStoresAction.INSTANCE, request, listener);
        }

        @Override
        public IndicesShardStoreRequestBuilder prepareShardStores(String... indices) {
            return new IndicesShardStoreRequestBuilder(this, IndicesShardStoresAction.INSTANCE, indices);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> updateSettings(final UpdateSettingsRequest request) {
            return execute(UpdateSettingsAction.INSTANCE, request);
        }

        @Override
        public void updateSettings(final UpdateSettingsRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(UpdateSettingsAction.INSTANCE, request, listener);
        }

        @Override
        public UpdateSettingsRequestBuilder prepareUpdateSettings(String... indices) {
            return new UpdateSettingsRequestBuilder(this, UpdateSettingsAction.INSTANCE).setIndices(indices);
        }

        @Override
        public ActionFuture<AnalyzeAction.Response> analyze(final AnalyzeAction.Request request) {
            return execute(AnalyzeAction.INSTANCE, request);
        }

        @Override
        public void analyze(final AnalyzeAction.Request request, final ActionListener<AnalyzeAction.Response> listener) {
            execute(AnalyzeAction.INSTANCE, request, listener);
        }

        @Override
        public AnalyzeRequestBuilder prepareAnalyze(@Nullable String index, String text) {
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE, index, text);
        }

        @Override
        public AnalyzeRequestBuilder prepareAnalyze(String text) {
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE, null, text);
        }

        @Override
        public AnalyzeRequestBuilder prepareAnalyze() {
            return new AnalyzeRequestBuilder(this, AnalyzeAction.INSTANCE);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> putTemplate(final PutIndexTemplateRequest request) {
            return execute(PutIndexTemplateAction.INSTANCE, request);
        }

        @Override
        public void putTemplate(final PutIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(PutIndexTemplateAction.INSTANCE, request, listener);
        }

        @Override
        public PutIndexTemplateRequestBuilder preparePutTemplate(String name) {
            return new PutIndexTemplateRequestBuilder(this, PutIndexTemplateAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<GetIndexTemplatesResponse> getTemplates(final GetIndexTemplatesRequest request) {
            return execute(GetIndexTemplatesAction.INSTANCE, request);
        }

        @Override
        public void getTemplates(final GetIndexTemplatesRequest request, final ActionListener<GetIndexTemplatesResponse> listener) {
            execute(GetIndexTemplatesAction.INSTANCE, request, listener);
        }

        @Override
        public GetIndexTemplatesRequestBuilder prepareGetTemplates(String... names) {
            return new GetIndexTemplatesRequestBuilder(this, GetIndexTemplatesAction.INSTANCE, names);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> deleteTemplate(final DeleteIndexTemplateRequest request) {
            return execute(DeleteIndexTemplateAction.INSTANCE, request);
        }

        @Override
        public void deleteTemplate(final DeleteIndexTemplateRequest request, final ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteIndexTemplateAction.INSTANCE, request, listener);
        }

        @Override
        public DeleteIndexTemplateRequestBuilder prepareDeleteTemplate(String name) {
            return new DeleteIndexTemplateRequestBuilder(this, DeleteIndexTemplateAction.INSTANCE, name);
        }

        @Override
        public ActionFuture<ValidateQueryResponse> validateQuery(final ValidateQueryRequest request) {
            return execute(ValidateQueryAction.INSTANCE, request);
        }

        @Override
        public void validateQuery(final ValidateQueryRequest request, final ActionListener<ValidateQueryResponse> listener) {
            execute(ValidateQueryAction.INSTANCE, request, listener);
        }

        @Override
        public ValidateQueryRequestBuilder prepareValidateQuery(String... indices) {
            return new ValidateQueryRequestBuilder(this, ValidateQueryAction.INSTANCE).setIndices(indices);
        }

        @Override
        public GetSettingsRequestBuilder prepareGetSettings(String... indices) {
            return new GetSettingsRequestBuilder(this, GetSettingsAction.INSTANCE, indices);
        }

        @Override
        public ResizeRequestBuilder prepareResizeIndex(String sourceIndex, String targetIndex) {
            return new ResizeRequestBuilder(this, ResizeAction.INSTANCE).setSourceIndex(sourceIndex)
                .setTargetIndex(new CreateIndexRequest(targetIndex));
        }

        @Override
        public ActionFuture<ResizeResponse> resizeIndex(ResizeRequest request) {
            return execute(ResizeAction.INSTANCE, request);
        }

        @Override
        public void resizeIndex(ResizeRequest request, ActionListener<ResizeResponse> listener) {
            execute(ResizeAction.INSTANCE, request, listener);
        }

        @Override
        public RolloverRequestBuilder prepareRolloverIndex(String alias) {
            return new RolloverRequestBuilder(this, RolloverAction.INSTANCE).setRolloverTarget(alias);
        }

        @Override
        public ActionFuture<RolloverResponse> rolloverIndex(RolloverRequest request) {
            return execute(RolloverAction.INSTANCE, request);
        }

        @Override
        public void rolloverIndex(RolloverRequest request, ActionListener<RolloverResponse> listener) {
            execute(RolloverAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<GetSettingsResponse> getSettings(GetSettingsRequest request) {
            return execute(GetSettingsAction.INSTANCE, request);
        }

        @Override
        public void getSettings(GetSettingsRequest request, ActionListener<GetSettingsResponse> listener) {
            execute(GetSettingsAction.INSTANCE, request, listener);
        }

        @Override
        public void createDataStream(CreateDataStreamAction.Request request, ActionListener<AcknowledgedResponse> listener) {
            execute(CreateDataStreamAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> createDataStream(CreateDataStreamAction.Request request) {
            return execute(CreateDataStreamAction.INSTANCE, request);
        }

        @Override
        public void deleteDataStream(DeleteDataStreamAction.Request request, ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteDataStreamAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<AcknowledgedResponse> deleteDataStream(DeleteDataStreamAction.Request request) {
            return execute(DeleteDataStreamAction.INSTANCE, request);
        }

        @Override
        public void getDataStreams(GetDataStreamAction.Request request, ActionListener<GetDataStreamAction.Response> listener) {
            execute(GetDataStreamAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<GetDataStreamAction.Response> getDataStreams(GetDataStreamAction.Request request) {
            return execute(GetDataStreamAction.INSTANCE, request);
        }

        @Override
        public void resolveIndex(ResolveIndexAction.Request request, ActionListener<ResolveIndexAction.Response> listener) {
            execute(ResolveIndexAction.INSTANCE, request, listener);
        }

        @Override
        public ActionFuture<ResolveIndexAction.Response> resolveIndex(ResolveIndexAction.Request request) {
            return execute(ResolveIndexAction.INSTANCE, request);
        }

        @Override
        public void createView(CreateViewAction.Request request, ActionListener<GetViewAction.Response> listener) {
            execute(CreateViewAction.INSTANCE, request);
        }

        @Override
        public ActionFuture<GetViewAction.Response> createView(CreateViewAction.Request request) {
            return execute(CreateViewAction.INSTANCE, request);
        }

        /** Gets a view */
        public void getView(GetViewAction.Request request, ActionListener<GetViewAction.Response> listener) {
            execute(GetViewAction.INSTANCE, request, listener);
        }

        /** Gets a view */
        public ActionFuture<GetViewAction.Response> getView(GetViewAction.Request request) {
            return execute(GetViewAction.INSTANCE, request);
        }

        /** Create a view */
        public void deleteView(DeleteViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
            execute(DeleteViewAction.INSTANCE, request, listener);
        }

        /** Create a view */
        public ActionFuture<AcknowledgedResponse> deleteView(DeleteViewAction.Request request) {
            return execute(DeleteViewAction.INSTANCE, request);
        }

        /** Create a view */
        public void updateView(CreateViewAction.Request request, ActionListener<GetViewAction.Response> listener) {
            execute(UpdateViewAction.INSTANCE, request, listener);
        }

        public ScaleIndexRequestBuilder prepareScaleSearchOnly(String index, boolean searchOnly) {
            return new ScaleIndexRequestBuilder(this, searchOnly, index);
        }

        /** Create a view */
        public ActionFuture<GetViewAction.Response> updateView(CreateViewAction.Request request) {
            return execute(UpdateViewAction.INSTANCE, request);
        }

        /** Pause ingestion */
        @Override
        public ActionFuture<PauseIngestionResponse> pauseIngestion(final PauseIngestionRequest request) {
            return execute(PauseIngestionAction.INSTANCE, request);
        }

        /** Pause ingestion */
        @Override
        public void pauseIngestion(final PauseIngestionRequest request, final ActionListener<PauseIngestionResponse> listener) {
            execute(PauseIngestionAction.INSTANCE, request, listener);
        }

        /** Resume ingestion */
        @Override
        public ActionFuture<ResumeIngestionResponse> resumeIngestion(final ResumeIngestionRequest request) {
            return execute(ResumeIngestionAction.INSTANCE, request);
        }

        /** Resume ingestion */
        @Override
        public void resumeIngestion(final ResumeIngestionRequest request, final ActionListener<ResumeIngestionResponse> listener) {
            execute(ResumeIngestionAction.INSTANCE, request, listener);
        }

        /** Get ingestion state */
        @Override
        public ActionFuture<GetIngestionStateResponse> getIngestionState(final GetIngestionStateRequest request) {
            return execute(GetIngestionStateAction.INSTANCE, request);
        }

        /** Get ingestion state */
        @Override
        public void getIngestionState(final GetIngestionStateRequest request, final ActionListener<GetIngestionStateResponse> listener) {
            execute(GetIngestionStateAction.INSTANCE, request, listener);
        }
    }

    @Override
    public Client filterWithHeader(Map<String, String> headers) {
        return new FilterClient(this) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                ThreadContext threadContext = threadPool().getThreadContext();
                try (
                    ThreadContext.StoredContext ctx = ThreadContextAccess.doPrivileged(() -> threadContext.stashAndMergeHeaders(headers))
                ) {
                    super.doExecute(action, request, listener);
                }
            }
        };
    }
}
