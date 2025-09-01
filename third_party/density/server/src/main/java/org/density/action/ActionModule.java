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

package org.density.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.admin.cluster.allocation.ClusterAllocationExplainAction;
import org.density.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.density.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.density.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.density.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.density.action.admin.cluster.configuration.TransportClearVotingConfigExclusionsAction;
import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateAction;
import org.density.action.admin.cluster.decommission.awareness.delete.TransportDeleteDecommissionStateAction;
import org.density.action.admin.cluster.decommission.awareness.get.GetDecommissionStateAction;
import org.density.action.admin.cluster.decommission.awareness.get.TransportGetDecommissionStateAction;
import org.density.action.admin.cluster.decommission.awareness.put.DecommissionAction;
import org.density.action.admin.cluster.decommission.awareness.put.TransportDecommissionAction;
import org.density.action.admin.cluster.health.ClusterHealthAction;
import org.density.action.admin.cluster.health.TransportClusterHealthAction;
import org.density.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.density.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.density.action.admin.cluster.node.info.NodesInfoAction;
import org.density.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.density.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.density.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.density.action.admin.cluster.node.reload.TransportNodesReloadSecureSettingsAction;
import org.density.action.admin.cluster.node.stats.NodesStatsAction;
import org.density.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.density.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.density.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.density.action.admin.cluster.node.tasks.get.GetTaskAction;
import org.density.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.density.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.density.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.density.action.admin.cluster.node.usage.NodesUsageAction;
import org.density.action.admin.cluster.node.usage.TransportNodesUsageAction;
import org.density.action.admin.cluster.remote.RemoteInfoAction;
import org.density.action.admin.cluster.remote.TransportRemoteInfoAction;
import org.density.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataAction;
import org.density.action.admin.cluster.remotestore.metadata.TransportRemoteStoreMetadataAction;
import org.density.action.admin.cluster.remotestore.restore.RestoreRemoteStoreAction;
import org.density.action.admin.cluster.remotestore.restore.TransportRestoreRemoteStoreAction;
import org.density.action.admin.cluster.remotestore.stats.RemoteStoreStatsAction;
import org.density.action.admin.cluster.remotestore.stats.TransportRemoteStoreStatsAction;
import org.density.action.admin.cluster.repositories.cleanup.CleanupRepositoryAction;
import org.density.action.admin.cluster.repositories.cleanup.TransportCleanupRepositoryAction;
import org.density.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.density.action.admin.cluster.repositories.delete.TransportDeleteRepositoryAction;
import org.density.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.density.action.admin.cluster.repositories.get.TransportGetRepositoriesAction;
import org.density.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.density.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.density.action.admin.cluster.repositories.verify.TransportVerifyRepositoryAction;
import org.density.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.density.action.admin.cluster.reroute.ClusterRerouteAction;
import org.density.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.density.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.density.action.admin.cluster.settings.TransportClusterUpdateSettingsAction;
import org.density.action.admin.cluster.shards.CatShardsAction;
import org.density.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.density.action.admin.cluster.shards.TransportCatShardsAction;
import org.density.action.admin.cluster.shards.TransportClusterSearchShardsAction;
import org.density.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingAction;
import org.density.action.admin.cluster.shards.routing.weighted.delete.TransportDeleteWeightedRoutingAction;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingAction;
import org.density.action.admin.cluster.shards.routing.weighted.get.TransportGetWeightedRoutingAction;
import org.density.action.admin.cluster.shards.routing.weighted.put.ClusterAddWeightedRoutingAction;
import org.density.action.admin.cluster.shards.routing.weighted.put.TransportAddWeightedRoutingAction;
import org.density.action.admin.cluster.snapshots.clone.CloneSnapshotAction;
import org.density.action.admin.cluster.snapshots.clone.TransportCloneSnapshotAction;
import org.density.action.admin.cluster.snapshots.create.CreateSnapshotAction;
import org.density.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.density.action.admin.cluster.snapshots.delete.DeleteSnapshotAction;
import org.density.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.density.action.admin.cluster.snapshots.get.GetSnapshotsAction;
import org.density.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.density.action.admin.cluster.snapshots.restore.RestoreSnapshotAction;
import org.density.action.admin.cluster.snapshots.restore.TransportRestoreSnapshotAction;
import org.density.action.admin.cluster.snapshots.status.SnapshotsStatusAction;
import org.density.action.admin.cluster.snapshots.status.TransportSnapshotsStatusAction;
import org.density.action.admin.cluster.state.ClusterStateAction;
import org.density.action.admin.cluster.state.TransportClusterStateAction;
import org.density.action.admin.cluster.stats.ClusterStatsAction;
import org.density.action.admin.cluster.stats.TransportClusterStatsAction;
import org.density.action.admin.cluster.storedscripts.DeleteStoredScriptAction;
import org.density.action.admin.cluster.storedscripts.GetScriptContextAction;
import org.density.action.admin.cluster.storedscripts.GetScriptLanguageAction;
import org.density.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.density.action.admin.cluster.storedscripts.PutStoredScriptAction;
import org.density.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.density.action.admin.cluster.storedscripts.TransportGetScriptContextAction;
import org.density.action.admin.cluster.storedscripts.TransportGetScriptLanguageAction;
import org.density.action.admin.cluster.storedscripts.TransportGetStoredScriptAction;
import org.density.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.density.action.admin.cluster.tasks.PendingClusterTasksAction;
import org.density.action.admin.cluster.tasks.TransportPendingClusterTasksAction;
import org.density.action.admin.cluster.wlm.TransportWlmStatsAction;
import org.density.action.admin.cluster.wlm.WlmStatsAction;
import org.density.action.admin.indices.alias.IndicesAliasesAction;
import org.density.action.admin.indices.alias.IndicesAliasesRequest;
import org.density.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.density.action.admin.indices.alias.get.GetAliasesAction;
import org.density.action.admin.indices.alias.get.TransportGetAliasesAction;
import org.density.action.admin.indices.analyze.AnalyzeAction;
import org.density.action.admin.indices.analyze.TransportAnalyzeAction;
import org.density.action.admin.indices.cache.clear.ClearIndicesCacheAction;
import org.density.action.admin.indices.cache.clear.TransportClearIndicesCacheAction;
import org.density.action.admin.indices.close.CloseIndexAction;
import org.density.action.admin.indices.close.TransportCloseIndexAction;
import org.density.action.admin.indices.create.AutoCreateAction;
import org.density.action.admin.indices.create.CreateIndexAction;
import org.density.action.admin.indices.create.TransportCreateIndexAction;
import org.density.action.admin.indices.dangling.delete.DeleteDanglingIndexAction;
import org.density.action.admin.indices.dangling.delete.TransportDeleteDanglingIndexAction;
import org.density.action.admin.indices.dangling.find.FindDanglingIndexAction;
import org.density.action.admin.indices.dangling.find.TransportFindDanglingIndexAction;
import org.density.action.admin.indices.dangling.import_index.ImportDanglingIndexAction;
import org.density.action.admin.indices.dangling.import_index.TransportImportDanglingIndexAction;
import org.density.action.admin.indices.dangling.list.ListDanglingIndicesAction;
import org.density.action.admin.indices.dangling.list.TransportListDanglingIndicesAction;
import org.density.action.admin.indices.datastream.CreateDataStreamAction;
import org.density.action.admin.indices.datastream.DataStreamsStatsAction;
import org.density.action.admin.indices.datastream.DeleteDataStreamAction;
import org.density.action.admin.indices.datastream.GetDataStreamAction;
import org.density.action.admin.indices.delete.DeleteIndexAction;
import org.density.action.admin.indices.delete.TransportDeleteIndexAction;
import org.density.action.admin.indices.exists.indices.IndicesExistsAction;
import org.density.action.admin.indices.exists.indices.TransportIndicesExistsAction;
import org.density.action.admin.indices.flush.FlushAction;
import org.density.action.admin.indices.flush.TransportFlushAction;
import org.density.action.admin.indices.forcemerge.ForceMergeAction;
import org.density.action.admin.indices.forcemerge.TransportForceMergeAction;
import org.density.action.admin.indices.get.GetIndexAction;
import org.density.action.admin.indices.get.TransportGetIndexAction;
import org.density.action.admin.indices.mapping.get.GetFieldMappingsAction;
import org.density.action.admin.indices.mapping.get.GetMappingsAction;
import org.density.action.admin.indices.mapping.get.TransportGetFieldMappingsAction;
import org.density.action.admin.indices.mapping.get.TransportGetFieldMappingsIndexAction;
import org.density.action.admin.indices.mapping.get.TransportGetMappingsAction;
import org.density.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.density.action.admin.indices.mapping.put.PutMappingAction;
import org.density.action.admin.indices.mapping.put.PutMappingRequest;
import org.density.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.density.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.density.action.admin.indices.open.OpenIndexAction;
import org.density.action.admin.indices.open.TransportOpenIndexAction;
import org.density.action.admin.indices.readonly.AddIndexBlockAction;
import org.density.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.density.action.admin.indices.recovery.RecoveryAction;
import org.density.action.admin.indices.recovery.TransportRecoveryAction;
import org.density.action.admin.indices.refresh.RefreshAction;
import org.density.action.admin.indices.refresh.TransportRefreshAction;
import org.density.action.admin.indices.replication.SegmentReplicationStatsAction;
import org.density.action.admin.indices.replication.TransportSegmentReplicationStatsAction;
import org.density.action.admin.indices.resolve.ResolveIndexAction;
import org.density.action.admin.indices.rollover.RolloverAction;
import org.density.action.admin.indices.rollover.TransportRolloverAction;
import org.density.action.admin.indices.scale.searchonly.ScaleIndexAction;
import org.density.action.admin.indices.scale.searchonly.TransportScaleIndexAction;
import org.density.action.admin.indices.segments.IndicesSegmentsAction;
import org.density.action.admin.indices.segments.PitSegmentsAction;
import org.density.action.admin.indices.segments.TransportIndicesSegmentsAction;
import org.density.action.admin.indices.segments.TransportPitSegmentsAction;
import org.density.action.admin.indices.settings.get.GetSettingsAction;
import org.density.action.admin.indices.settings.get.TransportGetSettingsAction;
import org.density.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.density.action.admin.indices.settings.put.UpdateSettingsAction;
import org.density.action.admin.indices.shards.IndicesShardStoresAction;
import org.density.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.density.action.admin.indices.shrink.ResizeAction;
import org.density.action.admin.indices.shrink.TransportResizeAction;
import org.density.action.admin.indices.stats.IndicesStatsAction;
import org.density.action.admin.indices.stats.TransportIndicesStatsAction;
import org.density.action.admin.indices.streamingingestion.pause.PauseIngestionAction;
import org.density.action.admin.indices.streamingingestion.pause.TransportPauseIngestionAction;
import org.density.action.admin.indices.streamingingestion.resume.ResumeIngestionAction;
import org.density.action.admin.indices.streamingingestion.resume.TransportResumeIngestionAction;
import org.density.action.admin.indices.streamingingestion.state.GetIngestionStateAction;
import org.density.action.admin.indices.streamingingestion.state.TransportGetIngestionStateAction;
import org.density.action.admin.indices.streamingingestion.state.TransportUpdateIngestionStateAction;
import org.density.action.admin.indices.streamingingestion.state.UpdateIngestionStateAction;
import org.density.action.admin.indices.template.delete.DeleteComponentTemplateAction;
import org.density.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction;
import org.density.action.admin.indices.template.delete.DeleteIndexTemplateAction;
import org.density.action.admin.indices.template.delete.TransportDeleteComponentTemplateAction;
import org.density.action.admin.indices.template.delete.TransportDeleteComposableIndexTemplateAction;
import org.density.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;
import org.density.action.admin.indices.template.get.GetComponentTemplateAction;
import org.density.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.density.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.density.action.admin.indices.template.get.TransportGetComponentTemplateAction;
import org.density.action.admin.indices.template.get.TransportGetComposableIndexTemplateAction;
import org.density.action.admin.indices.template.get.TransportGetIndexTemplatesAction;
import org.density.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.density.action.admin.indices.template.post.SimulateTemplateAction;
import org.density.action.admin.indices.template.post.TransportSimulateIndexTemplateAction;
import org.density.action.admin.indices.template.post.TransportSimulateTemplateAction;
import org.density.action.admin.indices.template.put.PutComponentTemplateAction;
import org.density.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.density.action.admin.indices.template.put.PutIndexTemplateAction;
import org.density.action.admin.indices.template.put.TransportPutComponentTemplateAction;
import org.density.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.density.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.density.action.admin.indices.upgrade.get.TransportUpgradeStatusAction;
import org.density.action.admin.indices.upgrade.get.UpgradeStatusAction;
import org.density.action.admin.indices.upgrade.post.TransportUpgradeAction;
import org.density.action.admin.indices.upgrade.post.TransportUpgradeSettingsAction;
import org.density.action.admin.indices.upgrade.post.UpgradeAction;
import org.density.action.admin.indices.upgrade.post.UpgradeSettingsAction;
import org.density.action.admin.indices.validate.query.TransportValidateQueryAction;
import org.density.action.admin.indices.validate.query.ValidateQueryAction;
import org.density.action.admin.indices.view.CreateViewAction;
import org.density.action.admin.indices.view.DeleteViewAction;
import org.density.action.admin.indices.view.GetViewAction;
import org.density.action.admin.indices.view.ListViewNamesAction;
import org.density.action.admin.indices.view.SearchViewAction;
import org.density.action.admin.indices.view.UpdateViewAction;
import org.density.action.bulk.BulkAction;
import org.density.action.bulk.TransportBulkAction;
import org.density.action.bulk.TransportShardBulkAction;
import org.density.action.delete.DeleteAction;
import org.density.action.delete.TransportDeleteAction;
import org.density.action.explain.ExplainAction;
import org.density.action.explain.TransportExplainAction;
import org.density.action.fieldcaps.FieldCapabilitiesAction;
import org.density.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.density.action.fieldcaps.TransportFieldCapabilitiesIndexAction;
import org.density.action.get.GetAction;
import org.density.action.get.MultiGetAction;
import org.density.action.get.TransportGetAction;
import org.density.action.get.TransportMultiGetAction;
import org.density.action.get.TransportShardMultiGetAction;
import org.density.action.index.IndexAction;
import org.density.action.index.TransportIndexAction;
import org.density.action.ingest.DeletePipelineAction;
import org.density.action.ingest.DeletePipelineTransportAction;
import org.density.action.ingest.GetPipelineAction;
import org.density.action.ingest.GetPipelineTransportAction;
import org.density.action.ingest.PutPipelineAction;
import org.density.action.ingest.PutPipelineTransportAction;
import org.density.action.ingest.SimulatePipelineAction;
import org.density.action.ingest.SimulatePipelineTransportAction;
import org.density.action.main.MainAction;
import org.density.action.main.TransportMainAction;
import org.density.action.search.ClearScrollAction;
import org.density.action.search.CreatePitAction;
import org.density.action.search.DeletePitAction;
import org.density.action.search.DeleteSearchPipelineAction;
import org.density.action.search.DeleteSearchPipelineTransportAction;
import org.density.action.search.GetAllPitsAction;
import org.density.action.search.GetSearchPipelineAction;
import org.density.action.search.GetSearchPipelineTransportAction;
import org.density.action.search.MultiSearchAction;
import org.density.action.search.PutSearchPipelineAction;
import org.density.action.search.PutSearchPipelineTransportAction;
import org.density.action.search.SearchAction;
import org.density.action.search.SearchScrollAction;
import org.density.action.search.StreamSearchAction;
import org.density.action.search.StreamTransportSearchAction;
import org.density.action.search.TransportClearScrollAction;
import org.density.action.search.TransportCreatePitAction;
import org.density.action.search.TransportDeletePitAction;
import org.density.action.search.TransportGetAllPitsAction;
import org.density.action.search.TransportMultiSearchAction;
import org.density.action.search.TransportSearchAction;
import org.density.action.search.TransportSearchScrollAction;
import org.density.action.support.ActionFilters;
import org.density.action.support.AutoCreateIndex;
import org.density.action.support.DestructiveOperations;
import org.density.action.support.TransportAction;
import org.density.action.support.clustermanager.term.GetTermVersionAction;
import org.density.action.support.clustermanager.term.TransportGetTermVersionAction;
import org.density.action.termvectors.MultiTermVectorsAction;
import org.density.action.termvectors.TermVectorsAction;
import org.density.action.termvectors.TransportMultiTermVectorsAction;
import org.density.action.termvectors.TransportShardMultiTermsVectorAction;
import org.density.action.termvectors.TransportTermVectorsAction;
import org.density.action.update.TransportUpdateAction;
import org.density.action.update.UpdateAction;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.node.DiscoveryNodes;
import org.density.common.NamedRegistry;
import org.density.common.annotation.PublicApi;
import org.density.common.breaker.ResponseLimitSettings;
import org.density.common.inject.AbstractModule;
import org.density.common.inject.TypeLiteral;
import org.density.common.inject.multibindings.MapBinder;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.IndexScopedSettings;
import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.common.util.FeatureFlags;
import org.density.core.action.ActionResponse;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.extensions.ExtensionsManager;
import org.density.extensions.action.ExtensionProxyAction;
import org.density.extensions.action.ExtensionProxyTransportAction;
import org.density.extensions.rest.RestInitializeExtensionAction;
import org.density.extensions.rest.RestSendToExtensionAction;
import org.density.identity.IdentityService;
import org.density.index.seqno.RetentionLeaseActions;
import org.density.indices.SystemIndices;
import org.density.persistent.CompletionPersistentTaskAction;
import org.density.persistent.RemovePersistentTaskAction;
import org.density.persistent.StartPersistentTaskAction;
import org.density.persistent.UpdatePersistentTaskStatusAction;
import org.density.plugins.ActionPlugin;
import org.density.plugins.ActionPlugin.ActionHandler;
import org.density.rest.NamedRoute;
import org.density.rest.RestController;
import org.density.rest.RestHandler;
import org.density.rest.RestHeaderDefinition;
import org.density.rest.action.RestFieldCapabilitiesAction;
import org.density.rest.action.RestMainAction;
import org.density.rest.action.admin.cluster.RestAddVotingConfigExclusionAction;
import org.density.rest.action.admin.cluster.RestCancelTasksAction;
import org.density.rest.action.admin.cluster.RestCleanupRepositoryAction;
import org.density.rest.action.admin.cluster.RestClearVotingConfigExclusionsAction;
import org.density.rest.action.admin.cluster.RestCloneSnapshotAction;
import org.density.rest.action.admin.cluster.RestClusterAllocationExplainAction;
import org.density.rest.action.admin.cluster.RestClusterDeleteWeightedRoutingAction;
import org.density.rest.action.admin.cluster.RestClusterGetSettingsAction;
import org.density.rest.action.admin.cluster.RestClusterGetWeightedRoutingAction;
import org.density.rest.action.admin.cluster.RestClusterHealthAction;
import org.density.rest.action.admin.cluster.RestClusterPutWeightedRoutingAction;
import org.density.rest.action.admin.cluster.RestClusterRerouteAction;
import org.density.rest.action.admin.cluster.RestClusterSearchShardsAction;
import org.density.rest.action.admin.cluster.RestClusterStateAction;
import org.density.rest.action.admin.cluster.RestClusterStatsAction;
import org.density.rest.action.admin.cluster.RestClusterUpdateSettingsAction;
import org.density.rest.action.admin.cluster.RestCreateSnapshotAction;
import org.density.rest.action.admin.cluster.RestDecommissionAction;
import org.density.rest.action.admin.cluster.RestDeleteDecommissionStateAction;
import org.density.rest.action.admin.cluster.RestDeleteRepositoryAction;
import org.density.rest.action.admin.cluster.RestDeleteSnapshotAction;
import org.density.rest.action.admin.cluster.RestDeleteStoredScriptAction;
import org.density.rest.action.admin.cluster.RestGetDecommissionStateAction;
import org.density.rest.action.admin.cluster.RestGetRepositoriesAction;
import org.density.rest.action.admin.cluster.RestGetScriptContextAction;
import org.density.rest.action.admin.cluster.RestGetScriptLanguageAction;
import org.density.rest.action.admin.cluster.RestGetSnapshotsAction;
import org.density.rest.action.admin.cluster.RestGetStoredScriptAction;
import org.density.rest.action.admin.cluster.RestGetTaskAction;
import org.density.rest.action.admin.cluster.RestListTasksAction;
import org.density.rest.action.admin.cluster.RestNodesHotThreadsAction;
import org.density.rest.action.admin.cluster.RestNodesInfoAction;
import org.density.rest.action.admin.cluster.RestNodesStatsAction;
import org.density.rest.action.admin.cluster.RestNodesUsageAction;
import org.density.rest.action.admin.cluster.RestPendingClusterTasksAction;
import org.density.rest.action.admin.cluster.RestPutRepositoryAction;
import org.density.rest.action.admin.cluster.RestPutStoredScriptAction;
import org.density.rest.action.admin.cluster.RestReloadSecureSettingsAction;
import org.density.rest.action.admin.cluster.RestRemoteClusterInfoAction;
import org.density.rest.action.admin.cluster.RestRemoteStoreMetadataAction;
import org.density.rest.action.admin.cluster.RestRemoteStoreStatsAction;
import org.density.rest.action.admin.cluster.RestRestoreRemoteStoreAction;
import org.density.rest.action.admin.cluster.RestRestoreSnapshotAction;
import org.density.rest.action.admin.cluster.RestSnapshotsStatusAction;
import org.density.rest.action.admin.cluster.RestVerifyRepositoryAction;
import org.density.rest.action.admin.cluster.RestWlmStatsAction;
import org.density.rest.action.admin.cluster.dangling.RestDeleteDanglingIndexAction;
import org.density.rest.action.admin.cluster.dangling.RestImportDanglingIndexAction;
import org.density.rest.action.admin.cluster.dangling.RestListDanglingIndicesAction;
import org.density.rest.action.admin.indices.RestAddIndexBlockAction;
import org.density.rest.action.admin.indices.RestAnalyzeAction;
import org.density.rest.action.admin.indices.RestClearIndicesCacheAction;
import org.density.rest.action.admin.indices.RestCloseIndexAction;
import org.density.rest.action.admin.indices.RestCreateDataStreamAction;
import org.density.rest.action.admin.indices.RestCreateIndexAction;
import org.density.rest.action.admin.indices.RestDataStreamsStatsAction;
import org.density.rest.action.admin.indices.RestDeleteComponentTemplateAction;
import org.density.rest.action.admin.indices.RestDeleteComposableIndexTemplateAction;
import org.density.rest.action.admin.indices.RestDeleteDataStreamAction;
import org.density.rest.action.admin.indices.RestDeleteIndexAction;
import org.density.rest.action.admin.indices.RestDeleteIndexTemplateAction;
import org.density.rest.action.admin.indices.RestFlushAction;
import org.density.rest.action.admin.indices.RestForceMergeAction;
import org.density.rest.action.admin.indices.RestGetAliasesAction;
import org.density.rest.action.admin.indices.RestGetComponentTemplateAction;
import org.density.rest.action.admin.indices.RestGetComposableIndexTemplateAction;
import org.density.rest.action.admin.indices.RestGetDataStreamsAction;
import org.density.rest.action.admin.indices.RestGetFieldMappingAction;
import org.density.rest.action.admin.indices.RestGetIndexTemplateAction;
import org.density.rest.action.admin.indices.RestGetIndicesAction;
import org.density.rest.action.admin.indices.RestGetIngestionStateAction;
import org.density.rest.action.admin.indices.RestGetMappingAction;
import org.density.rest.action.admin.indices.RestGetSettingsAction;
import org.density.rest.action.admin.indices.RestIndexDeleteAliasesAction;
import org.density.rest.action.admin.indices.RestIndexPutAliasAction;
import org.density.rest.action.admin.indices.RestIndicesAliasesAction;
import org.density.rest.action.admin.indices.RestIndicesSegmentsAction;
import org.density.rest.action.admin.indices.RestIndicesShardStoresAction;
import org.density.rest.action.admin.indices.RestIndicesStatsAction;
import org.density.rest.action.admin.indices.RestOpenIndexAction;
import org.density.rest.action.admin.indices.RestPauseIngestionAction;
import org.density.rest.action.admin.indices.RestPutComponentTemplateAction;
import org.density.rest.action.admin.indices.RestPutComposableIndexTemplateAction;
import org.density.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.density.rest.action.admin.indices.RestPutMappingAction;
import org.density.rest.action.admin.indices.RestRecoveryAction;
import org.density.rest.action.admin.indices.RestRefreshAction;
import org.density.rest.action.admin.indices.RestResizeHandler;
import org.density.rest.action.admin.indices.RestResolveIndexAction;
import org.density.rest.action.admin.indices.RestResumeIngestionAction;
import org.density.rest.action.admin.indices.RestRolloverIndexAction;
import org.density.rest.action.admin.indices.RestScaleIndexAction;
import org.density.rest.action.admin.indices.RestSimulateIndexTemplateAction;
import org.density.rest.action.admin.indices.RestSimulateTemplateAction;
import org.density.rest.action.admin.indices.RestSyncedFlushAction;
import org.density.rest.action.admin.indices.RestUpdateSettingsAction;
import org.density.rest.action.admin.indices.RestUpgradeAction;
import org.density.rest.action.admin.indices.RestUpgradeStatusAction;
import org.density.rest.action.admin.indices.RestValidateQueryAction;
import org.density.rest.action.admin.indices.RestViewAction;
import org.density.rest.action.cat.AbstractCatAction;
import org.density.rest.action.cat.RestAliasAction;
import org.density.rest.action.cat.RestAllocationAction;
import org.density.rest.action.cat.RestCatAction;
import org.density.rest.action.cat.RestCatRecoveryAction;
import org.density.rest.action.cat.RestCatSegmentReplicationAction;
import org.density.rest.action.cat.RestClusterManagerAction;
import org.density.rest.action.cat.RestFielddataAction;
import org.density.rest.action.cat.RestHealthAction;
import org.density.rest.action.cat.RestIndicesAction;
import org.density.rest.action.cat.RestNodeAttrsAction;
import org.density.rest.action.cat.RestNodesAction;
import org.density.rest.action.cat.RestPitSegmentsAction;
import org.density.rest.action.cat.RestPluginsAction;
import org.density.rest.action.cat.RestRepositoriesAction;
import org.density.rest.action.cat.RestSegmentsAction;
import org.density.rest.action.cat.RestShardsAction;
import org.density.rest.action.cat.RestSnapshotAction;
import org.density.rest.action.cat.RestTasksAction;
import org.density.rest.action.cat.RestTemplatesAction;
import org.density.rest.action.cat.RestThreadPoolAction;
import org.density.rest.action.document.RestBulkAction;
import org.density.rest.action.document.RestBulkStreamingAction;
import org.density.rest.action.document.RestDeleteAction;
import org.density.rest.action.document.RestGetAction;
import org.density.rest.action.document.RestGetSourceAction;
import org.density.rest.action.document.RestIndexAction;
import org.density.rest.action.document.RestIndexAction.AutoIdHandler;
import org.density.rest.action.document.RestIndexAction.CreateHandler;
import org.density.rest.action.document.RestMultiGetAction;
import org.density.rest.action.document.RestMultiTermVectorsAction;
import org.density.rest.action.document.RestTermVectorsAction;
import org.density.rest.action.document.RestUpdateAction;
import org.density.rest.action.ingest.RestDeletePipelineAction;
import org.density.rest.action.ingest.RestGetPipelineAction;
import org.density.rest.action.ingest.RestPutPipelineAction;
import org.density.rest.action.ingest.RestSimulatePipelineAction;
import org.density.rest.action.list.AbstractListAction;
import org.density.rest.action.list.RestIndicesListAction;
import org.density.rest.action.list.RestListAction;
import org.density.rest.action.list.RestShardsListAction;
import org.density.rest.action.search.RestClearScrollAction;
import org.density.rest.action.search.RestCountAction;
import org.density.rest.action.search.RestCreatePitAction;
import org.density.rest.action.search.RestDeletePitAction;
import org.density.rest.action.search.RestDeleteSearchPipelineAction;
import org.density.rest.action.search.RestExplainAction;
import org.density.rest.action.search.RestGetAllPitsAction;
import org.density.rest.action.search.RestGetSearchPipelineAction;
import org.density.rest.action.search.RestMultiSearchAction;
import org.density.rest.action.search.RestPutSearchPipelineAction;
import org.density.rest.action.search.RestSearchAction;
import org.density.rest.action.search.RestSearchScrollAction;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.node.NodeClient;
import org.density.usage.UsageService;
import org.density.wlm.WorkloadGroupTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

/**
 * Builds and binds the generic action map, all {@link TransportAction}s, and {@link ActionFilters}.
 *
 * @density.internal
 */
public class ActionModule extends AbstractModule {

    private static final Logger logger = LogManager.getLogger(ActionModule.class);

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final IndexScopedSettings indexScopedSettings;
    private final ClusterSettings clusterSettings;
    private final SettingsFilter settingsFilter;
    private final List<ActionPlugin> actionPlugins;
    // The unmodifiable map containing Density and Plugin actions
    // This is initialized at node bootstrap and contains same-JVM actions
    // It will be wrapped in the Dynamic Action Registry but otherwise
    // remains unchanged from its prior purpose, and registered actions
    // will remain accessible.
    private final Map<String, ActionHandler<?, ?>> actions;
    // A dynamic action registry which includes the above immutable actions
    // and also registers dynamic actions which may be unregistered. Usually
    // associated with remote action execution on extensions, possibly in
    // a different JVM and possibly on a different server.
    private final DynamicActionRegistry dynamicActionRegistry;
    private final ActionFilters actionFilters;
    private final AutoCreateIndex autoCreateIndex;
    private final DestructiveOperations destructiveOperations;
    private final RestController restController;
    private final RequestValidators<PutMappingRequest> mappingRequestValidators;
    private final RequestValidators<IndicesAliasesRequest> indicesAliasesRequestRequestValidators;
    private final ThreadPool threadPool;
    private final ExtensionsManager extensionsManager;
    private final ResponseLimitSettings responseLimitSettings;

    public ActionModule(
        Settings settings,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndexScopedSettings indexScopedSettings,
        ClusterSettings clusterSettings,
        SettingsFilter settingsFilter,
        ThreadPool threadPool,
        List<ActionPlugin> actionPlugins,
        NodeClient nodeClient,
        CircuitBreakerService circuitBreakerService,
        UsageService usageService,
        SystemIndices systemIndices,
        IdentityService identityService,
        ExtensionsManager extensionsManager
    ) {
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.indexScopedSettings = indexScopedSettings;
        this.clusterSettings = clusterSettings;
        this.settingsFilter = settingsFilter;
        this.actionPlugins = actionPlugins;
        this.threadPool = threadPool;
        this.extensionsManager = extensionsManager;
        actions = setupActions(actionPlugins);
        actionFilters = setupActionFilters(actionPlugins);
        dynamicActionRegistry = new DynamicActionRegistry();
        autoCreateIndex = new AutoCreateIndex(settings, clusterSettings, indexNameExpressionResolver, systemIndices);
        destructiveOperations = new DestructiveOperations(settings, clusterSettings);
        Set<RestHeaderDefinition> headers = Stream.concat(
            actionPlugins.stream().flatMap(p -> p.getRestHeaders().stream()),
            Stream.of(
                new RestHeaderDefinition(Task.X_OPAQUE_ID, false),
                new RestHeaderDefinition(WorkloadGroupTask.WORKLOAD_GROUP_ID_HEADER, false)
            )
        ).collect(Collectors.toSet());
        UnaryOperator<RestHandler> restWrapper = null;
        for (ActionPlugin plugin : actionPlugins) {
            UnaryOperator<RestHandler> newRestWrapper = plugin.getRestHandlerWrapper(threadPool.getThreadContext());
            if (newRestWrapper != null) {
                logger.debug("Using REST wrapper from plugin " + plugin.getClass().getName());
                if (restWrapper != null) {
                    throw new IllegalArgumentException("Cannot have more than one plugin implementing a REST wrapper");
                }
                restWrapper = newRestWrapper;
            }
        }
        mappingRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.mappingRequestValidators().stream()).collect(Collectors.toList())
        );
        indicesAliasesRequestRequestValidators = new RequestValidators<>(
            actionPlugins.stream().flatMap(p -> p.indicesAliasesRequestValidators().stream()).collect(Collectors.toList())
        );

        restController = new RestController(headers, restWrapper, nodeClient, circuitBreakerService, usageService);
        responseLimitSettings = new ResponseLimitSettings(clusterSettings, settings);
    }

    public Map<String, ActionHandler<?, ?>> getActions() {
        return actions;
    }

    static Map<String, ActionHandler<?, ?>> setupActions(List<ActionPlugin> actionPlugins) {
        // Subclass NamedRegistry for easy registration
        class ActionRegistry extends NamedRegistry<ActionHandler<?, ?>> {
            ActionRegistry() {
                super("action");
            }

            public void register(ActionHandler<?, ?> handler) {
                register(handler.getAction().name(), handler);
            }

            public <Request extends ActionRequest, Response extends ActionResponse> void register(
                ActionType<Response> action,
                Class<? extends TransportAction<Request, Response>> transportAction,
                Class<?>... supportTransportActions
            ) {
                register(new ActionHandler<>(action, transportAction, supportTransportActions));
            }
        }
        ActionRegistry actions = new ActionRegistry();

        actions.register(MainAction.INSTANCE, TransportMainAction.class);
        actions.register(NodesInfoAction.INSTANCE, TransportNodesInfoAction.class);
        actions.register(RemoteInfoAction.INSTANCE, TransportRemoteInfoAction.class);
        actions.register(NodesStatsAction.INSTANCE, TransportNodesStatsAction.class);
        actions.register(WlmStatsAction.INSTANCE, TransportWlmStatsAction.class);
        actions.register(RemoteStoreStatsAction.INSTANCE, TransportRemoteStoreStatsAction.class);
        actions.register(RemoteStoreMetadataAction.INSTANCE, TransportRemoteStoreMetadataAction.class);
        actions.register(NodesUsageAction.INSTANCE, TransportNodesUsageAction.class);
        actions.register(NodesHotThreadsAction.INSTANCE, TransportNodesHotThreadsAction.class);
        actions.register(ListTasksAction.INSTANCE, TransportListTasksAction.class);
        actions.register(GetTaskAction.INSTANCE, TransportGetTaskAction.class);
        actions.register(CancelTasksAction.INSTANCE, TransportCancelTasksAction.class);

        actions.register(AddVotingConfigExclusionsAction.INSTANCE, TransportAddVotingConfigExclusionsAction.class);
        actions.register(ClearVotingConfigExclusionsAction.INSTANCE, TransportClearVotingConfigExclusionsAction.class);
        actions.register(ClusterAllocationExplainAction.INSTANCE, TransportClusterAllocationExplainAction.class);
        actions.register(ClusterStatsAction.INSTANCE, TransportClusterStatsAction.class);
        actions.register(ClusterStateAction.INSTANCE, TransportClusterStateAction.class);
        actions.register(GetTermVersionAction.INSTANCE, TransportGetTermVersionAction.class);
        actions.register(ClusterHealthAction.INSTANCE, TransportClusterHealthAction.class);
        actions.register(ClusterUpdateSettingsAction.INSTANCE, TransportClusterUpdateSettingsAction.class);
        actions.register(ClusterRerouteAction.INSTANCE, TransportClusterRerouteAction.class);
        actions.register(ClusterSearchShardsAction.INSTANCE, TransportClusterSearchShardsAction.class);
        actions.register(PendingClusterTasksAction.INSTANCE, TransportPendingClusterTasksAction.class);
        actions.register(PutRepositoryAction.INSTANCE, TransportPutRepositoryAction.class);
        actions.register(GetRepositoriesAction.INSTANCE, TransportGetRepositoriesAction.class);
        actions.register(DeleteRepositoryAction.INSTANCE, TransportDeleteRepositoryAction.class);
        actions.register(VerifyRepositoryAction.INSTANCE, TransportVerifyRepositoryAction.class);
        actions.register(CleanupRepositoryAction.INSTANCE, TransportCleanupRepositoryAction.class);
        actions.register(GetSnapshotsAction.INSTANCE, TransportGetSnapshotsAction.class);
        actions.register(DeleteSnapshotAction.INSTANCE, TransportDeleteSnapshotAction.class);
        actions.register(CreateSnapshotAction.INSTANCE, TransportCreateSnapshotAction.class);
        actions.register(CloneSnapshotAction.INSTANCE, TransportCloneSnapshotAction.class);
        actions.register(RestoreSnapshotAction.INSTANCE, TransportRestoreSnapshotAction.class);
        actions.register(SnapshotsStatusAction.INSTANCE, TransportSnapshotsStatusAction.class);

        actions.register(ClusterAddWeightedRoutingAction.INSTANCE, TransportAddWeightedRoutingAction.class);
        actions.register(ClusterGetWeightedRoutingAction.INSTANCE, TransportGetWeightedRoutingAction.class);
        actions.register(ClusterDeleteWeightedRoutingAction.INSTANCE, TransportDeleteWeightedRoutingAction.class);
        actions.register(IndicesStatsAction.INSTANCE, TransportIndicesStatsAction.class);
        actions.register(CatShardsAction.INSTANCE, TransportCatShardsAction.class);
        actions.register(IndicesSegmentsAction.INSTANCE, TransportIndicesSegmentsAction.class);
        actions.register(IndicesShardStoresAction.INSTANCE, TransportIndicesShardStoresAction.class);
        actions.register(CreateIndexAction.INSTANCE, TransportCreateIndexAction.class);
        actions.register(ResizeAction.INSTANCE, TransportResizeAction.class);
        actions.register(RolloverAction.INSTANCE, TransportRolloverAction.class);
        actions.register(DeleteIndexAction.INSTANCE, TransportDeleteIndexAction.class);
        actions.register(GetIndexAction.INSTANCE, TransportGetIndexAction.class);
        actions.register(OpenIndexAction.INSTANCE, TransportOpenIndexAction.class);
        actions.register(CloseIndexAction.INSTANCE, TransportCloseIndexAction.class);
        actions.register(IndicesExistsAction.INSTANCE, TransportIndicesExistsAction.class);
        actions.register(AddIndexBlockAction.INSTANCE, TransportAddIndexBlockAction.class);
        actions.register(GetMappingsAction.INSTANCE, TransportGetMappingsAction.class);
        actions.register(
            GetFieldMappingsAction.INSTANCE,
            TransportGetFieldMappingsAction.class,
            TransportGetFieldMappingsIndexAction.class
        );
        actions.register(PutMappingAction.INSTANCE, TransportPutMappingAction.class);
        actions.register(AutoPutMappingAction.INSTANCE, TransportAutoPutMappingAction.class);
        actions.register(IndicesAliasesAction.INSTANCE, TransportIndicesAliasesAction.class);
        actions.register(UpdateSettingsAction.INSTANCE, TransportUpdateSettingsAction.class);
        actions.register(ScaleIndexAction.INSTANCE, TransportScaleIndexAction.class);
        actions.register(AnalyzeAction.INSTANCE, TransportAnalyzeAction.class);
        actions.register(PutIndexTemplateAction.INSTANCE, TransportPutIndexTemplateAction.class);
        actions.register(GetIndexTemplatesAction.INSTANCE, TransportGetIndexTemplatesAction.class);
        actions.register(DeleteIndexTemplateAction.INSTANCE, TransportDeleteIndexTemplateAction.class);
        actions.register(PutComponentTemplateAction.INSTANCE, TransportPutComponentTemplateAction.class);
        actions.register(GetComponentTemplateAction.INSTANCE, TransportGetComponentTemplateAction.class);
        actions.register(DeleteComponentTemplateAction.INSTANCE, TransportDeleteComponentTemplateAction.class);
        actions.register(PutComposableIndexTemplateAction.INSTANCE, TransportPutComposableIndexTemplateAction.class);
        actions.register(GetComposableIndexTemplateAction.INSTANCE, TransportGetComposableIndexTemplateAction.class);
        actions.register(DeleteComposableIndexTemplateAction.INSTANCE, TransportDeleteComposableIndexTemplateAction.class);
        actions.register(SimulateIndexTemplateAction.INSTANCE, TransportSimulateIndexTemplateAction.class);
        actions.register(SimulateTemplateAction.INSTANCE, TransportSimulateTemplateAction.class);
        actions.register(ValidateQueryAction.INSTANCE, TransportValidateQueryAction.class);
        actions.register(RefreshAction.INSTANCE, TransportRefreshAction.class);
        actions.register(FlushAction.INSTANCE, TransportFlushAction.class);
        actions.register(ForceMergeAction.INSTANCE, TransportForceMergeAction.class);
        actions.register(UpgradeAction.INSTANCE, TransportUpgradeAction.class);
        actions.register(UpgradeStatusAction.INSTANCE, TransportUpgradeStatusAction.class);
        actions.register(UpgradeSettingsAction.INSTANCE, TransportUpgradeSettingsAction.class);
        actions.register(ClearIndicesCacheAction.INSTANCE, TransportClearIndicesCacheAction.class);
        actions.register(GetAliasesAction.INSTANCE, TransportGetAliasesAction.class);
        actions.register(GetSettingsAction.INSTANCE, TransportGetSettingsAction.class);

        actions.register(IndexAction.INSTANCE, TransportIndexAction.class);
        actions.register(GetAction.INSTANCE, TransportGetAction.class);
        actions.register(TermVectorsAction.INSTANCE, TransportTermVectorsAction.class);
        actions.register(
            MultiTermVectorsAction.INSTANCE,
            TransportMultiTermVectorsAction.class,
            TransportShardMultiTermsVectorAction.class
        );
        actions.register(DeleteAction.INSTANCE, TransportDeleteAction.class);
        actions.register(UpdateAction.INSTANCE, TransportUpdateAction.class);
        actions.register(MultiGetAction.INSTANCE, TransportMultiGetAction.class, TransportShardMultiGetAction.class);
        actions.register(BulkAction.INSTANCE, TransportBulkAction.class, TransportShardBulkAction.class);
        actions.register(SearchAction.INSTANCE, TransportSearchAction.class);
        if (FeatureFlags.isEnabled(FeatureFlags.STREAM_TRANSPORT)) {
            actions.register(StreamSearchAction.INSTANCE, StreamTransportSearchAction.class);
        }
        actions.register(SearchScrollAction.INSTANCE, TransportSearchScrollAction.class);
        actions.register(MultiSearchAction.INSTANCE, TransportMultiSearchAction.class);
        actions.register(ExplainAction.INSTANCE, TransportExplainAction.class);
        actions.register(ClearScrollAction.INSTANCE, TransportClearScrollAction.class);
        actions.register(RecoveryAction.INSTANCE, TransportRecoveryAction.class);
        actions.register(SegmentReplicationStatsAction.INSTANCE, TransportSegmentReplicationStatsAction.class);
        actions.register(NodesReloadSecureSettingsAction.INSTANCE, TransportNodesReloadSecureSettingsAction.class);
        actions.register(AutoCreateAction.INSTANCE, AutoCreateAction.TransportAction.class);

        // Indexed scripts
        actions.register(PutStoredScriptAction.INSTANCE, TransportPutStoredScriptAction.class);
        actions.register(GetStoredScriptAction.INSTANCE, TransportGetStoredScriptAction.class);
        actions.register(DeleteStoredScriptAction.INSTANCE, TransportDeleteStoredScriptAction.class);
        actions.register(GetScriptContextAction.INSTANCE, TransportGetScriptContextAction.class);
        actions.register(GetScriptLanguageAction.INSTANCE, TransportGetScriptLanguageAction.class);

        actions.register(
            FieldCapabilitiesAction.INSTANCE,
            TransportFieldCapabilitiesAction.class,
            TransportFieldCapabilitiesIndexAction.class
        );

        actions.register(PutPipelineAction.INSTANCE, PutPipelineTransportAction.class);
        actions.register(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
        actions.register(DeletePipelineAction.INSTANCE, DeletePipelineTransportAction.class);
        actions.register(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);

        actionPlugins.stream().flatMap(p -> p.getActions().stream()).forEach(actions::register);

        // Data streams:
        actions.register(CreateDataStreamAction.INSTANCE, CreateDataStreamAction.TransportAction.class);
        actions.register(DeleteDataStreamAction.INSTANCE, DeleteDataStreamAction.TransportAction.class);
        actions.register(GetDataStreamAction.INSTANCE, GetDataStreamAction.TransportAction.class);
        actions.register(ResolveIndexAction.INSTANCE, ResolveIndexAction.TransportAction.class);
        actions.register(DataStreamsStatsAction.INSTANCE, DataStreamsStatsAction.TransportAction.class);

        // Views:
        actions.register(CreateViewAction.INSTANCE, CreateViewAction.TransportAction.class);
        actions.register(DeleteViewAction.INSTANCE, DeleteViewAction.TransportAction.class);
        actions.register(GetViewAction.INSTANCE, GetViewAction.TransportAction.class);
        actions.register(UpdateViewAction.INSTANCE, UpdateViewAction.TransportAction.class);
        actions.register(ListViewNamesAction.INSTANCE, ListViewNamesAction.TransportAction.class);
        actions.register(SearchViewAction.INSTANCE, SearchViewAction.TransportAction.class);

        // Persistent tasks:
        actions.register(StartPersistentTaskAction.INSTANCE, StartPersistentTaskAction.TransportAction.class);
        actions.register(UpdatePersistentTaskStatusAction.INSTANCE, UpdatePersistentTaskStatusAction.TransportAction.class);
        actions.register(CompletionPersistentTaskAction.INSTANCE, CompletionPersistentTaskAction.TransportAction.class);
        actions.register(RemovePersistentTaskAction.INSTANCE, RemovePersistentTaskAction.TransportAction.class);

        // retention leases
        actions.register(RetentionLeaseActions.Add.INSTANCE, RetentionLeaseActions.Add.TransportAction.class);
        actions.register(RetentionLeaseActions.Renew.INSTANCE, RetentionLeaseActions.Renew.TransportAction.class);
        actions.register(RetentionLeaseActions.Remove.INSTANCE, RetentionLeaseActions.Remove.TransportAction.class);

        // Dangling indices
        actions.register(ListDanglingIndicesAction.INSTANCE, TransportListDanglingIndicesAction.class);
        actions.register(ImportDanglingIndexAction.INSTANCE, TransportImportDanglingIndexAction.class);
        actions.register(DeleteDanglingIndexAction.INSTANCE, TransportDeleteDanglingIndexAction.class);
        actions.register(FindDanglingIndexAction.INSTANCE, TransportFindDanglingIndexAction.class);

        // point in time actions
        actions.register(CreatePitAction.INSTANCE, TransportCreatePitAction.class);
        actions.register(DeletePitAction.INSTANCE, TransportDeletePitAction.class);
        actions.register(PitSegmentsAction.INSTANCE, TransportPitSegmentsAction.class);
        actions.register(GetAllPitsAction.INSTANCE, TransportGetAllPitsAction.class);

        // Remote Store
        actions.register(RestoreRemoteStoreAction.INSTANCE, TransportRestoreRemoteStoreAction.class);

        if (FeatureFlags.isEnabled(FeatureFlags.EXTENSIONS)) {
            // ExtensionProxyAction
            actions.register(ExtensionProxyAction.INSTANCE, ExtensionProxyTransportAction.class);
        }

        // Decommission actions
        actions.register(DecommissionAction.INSTANCE, TransportDecommissionAction.class);
        actions.register(GetDecommissionStateAction.INSTANCE, TransportGetDecommissionStateAction.class);
        actions.register(DeleteDecommissionStateAction.INSTANCE, TransportDeleteDecommissionStateAction.class);

        // Search Pipelines
        actions.register(PutSearchPipelineAction.INSTANCE, PutSearchPipelineTransportAction.class);
        actions.register(GetSearchPipelineAction.INSTANCE, GetSearchPipelineTransportAction.class);
        actions.register(DeleteSearchPipelineAction.INSTANCE, DeleteSearchPipelineTransportAction.class);

        // Pull-based ingestion actions
        actions.register(PauseIngestionAction.INSTANCE, TransportPauseIngestionAction.class);
        actions.register(ResumeIngestionAction.INSTANCE, TransportResumeIngestionAction.class);
        actions.register(GetIngestionStateAction.INSTANCE, TransportGetIngestionStateAction.class);
        actions.register(UpdateIngestionStateAction.INSTANCE, TransportUpdateIngestionStateAction.class);

        return unmodifiableMap(actions.getRegistry());
    }

    private ActionFilters setupActionFilters(List<ActionPlugin> actionPlugins) {
        return new ActionFilters(
            Collections.unmodifiableSet(actionPlugins.stream().flatMap(p -> p.getActionFilters().stream()).collect(Collectors.toSet()))
        );
    }

    public void initRestHandlers(Supplier<DiscoveryNodes> nodesInCluster) {
        List<AbstractCatAction> catActions = new ArrayList<>();
        List<AbstractListAction> listActions = new ArrayList<>();
        Consumer<RestHandler> registerHandler = handler -> {
            if (handler instanceof AbstractCatAction) {
                if (handler instanceof AbstractListAction && ((AbstractListAction) handler).isActionPaginated()) {
                    listActions.add((AbstractListAction) handler);
                } else {
                    catActions.add((AbstractCatAction) handler);
                }
            }
            restController.registerHandler(handler);
        };
        registerHandler.accept(new RestAddVotingConfigExclusionAction());
        registerHandler.accept(new RestClearVotingConfigExclusionsAction());
        registerHandler.accept(new RestMainAction());
        registerHandler.accept(new RestNodesInfoAction(settingsFilter));
        registerHandler.accept(new RestWlmStatsAction());
        registerHandler.accept(new RestRemoteClusterInfoAction());
        registerHandler.accept(new RestNodesStatsAction());
        registerHandler.accept(new RestNodesUsageAction());
        registerHandler.accept(new RestNodesHotThreadsAction());
        registerHandler.accept(new RestClusterAllocationExplainAction());
        registerHandler.accept(new RestClusterStatsAction());
        registerHandler.accept(new RestClusterStateAction(settingsFilter));
        registerHandler.accept(new RestClusterHealthAction());
        registerHandler.accept(new RestClusterUpdateSettingsAction());
        registerHandler.accept(new RestClusterGetSettingsAction(settings, clusterSettings, settingsFilter));
        registerHandler.accept(new RestClusterRerouteAction(settingsFilter));
        registerHandler.accept(new RestClusterSearchShardsAction());
        registerHandler.accept(new RestPendingClusterTasksAction());
        registerHandler.accept(new RestPutRepositoryAction());
        registerHandler.accept(new RestGetRepositoriesAction(settingsFilter));
        registerHandler.accept(new RestDeleteRepositoryAction());
        registerHandler.accept(new RestVerifyRepositoryAction());
        registerHandler.accept(new RestCleanupRepositoryAction());
        registerHandler.accept(new RestGetSnapshotsAction());
        registerHandler.accept(new RestCreateSnapshotAction());
        registerHandler.accept(new RestCloneSnapshotAction());
        registerHandler.accept(new RestRestoreSnapshotAction());
        registerHandler.accept(new RestDeleteSnapshotAction());
        registerHandler.accept(new RestSnapshotsStatusAction());
        registerHandler.accept(new RestGetIndicesAction());
        registerHandler.accept(new RestIndicesStatsAction());
        registerHandler.accept(new RestIndicesSegmentsAction());
        registerHandler.accept(new RestIndicesShardStoresAction());
        registerHandler.accept(new RestGetAliasesAction());
        registerHandler.accept(new RestIndexDeleteAliasesAction());
        registerHandler.accept(new RestIndexPutAliasAction());
        registerHandler.accept(new RestIndicesAliasesAction());
        registerHandler.accept(new RestCreateIndexAction());
        registerHandler.accept(new RestResizeHandler.RestShrinkIndexAction());
        registerHandler.accept(new RestResizeHandler.RestSplitIndexAction());
        registerHandler.accept(new RestResizeHandler.RestCloneIndexAction());
        registerHandler.accept(new RestRolloverIndexAction());
        registerHandler.accept(new RestDeleteIndexAction());
        registerHandler.accept(new RestCloseIndexAction());
        registerHandler.accept(new RestOpenIndexAction());
        registerHandler.accept(new RestAddIndexBlockAction());

        registerHandler.accept(new RestClusterPutWeightedRoutingAction());
        registerHandler.accept(new RestClusterGetWeightedRoutingAction());
        registerHandler.accept(new RestClusterDeleteWeightedRoutingAction());

        registerHandler.accept(new RestUpdateSettingsAction());
        registerHandler.accept(new RestGetSettingsAction());

        registerHandler.accept(new RestAnalyzeAction());
        registerHandler.accept(new RestGetIndexTemplateAction());
        registerHandler.accept(new RestPutIndexTemplateAction());
        registerHandler.accept(new RestDeleteIndexTemplateAction());
        registerHandler.accept(new RestPutComponentTemplateAction());
        registerHandler.accept(new RestGetComponentTemplateAction());
        registerHandler.accept(new RestDeleteComponentTemplateAction());
        registerHandler.accept(new RestPutComposableIndexTemplateAction());
        registerHandler.accept(new RestGetComposableIndexTemplateAction());
        registerHandler.accept(new RestDeleteComposableIndexTemplateAction());
        registerHandler.accept(new RestSimulateIndexTemplateAction());
        registerHandler.accept(new RestSimulateTemplateAction());

        registerHandler.accept(new RestPutMappingAction());
        registerHandler.accept(new RestGetMappingAction(threadPool));
        registerHandler.accept(new RestGetFieldMappingAction());

        registerHandler.accept(new RestRefreshAction());
        registerHandler.accept(new RestFlushAction());
        registerHandler.accept(new RestSyncedFlushAction());
        registerHandler.accept(new RestForceMergeAction());
        registerHandler.accept(new RestUpgradeAction());
        registerHandler.accept(new RestUpgradeStatusAction());
        registerHandler.accept(new RestClearIndicesCacheAction());
        registerHandler.accept(new RestScaleIndexAction());
        registerHandler.accept(new RestIndexAction());
        registerHandler.accept(new CreateHandler());
        registerHandler.accept(new AutoIdHandler(nodesInCluster));
        registerHandler.accept(new RestGetAction());
        registerHandler.accept(new RestGetSourceAction());
        registerHandler.accept(new RestMultiGetAction(settings));
        registerHandler.accept(new RestDeleteAction());
        registerHandler.accept(new RestCountAction());
        registerHandler.accept(new RestTermVectorsAction());
        registerHandler.accept(new RestMultiTermVectorsAction());
        registerHandler.accept(new RestBulkAction(settings));
        registerHandler.accept(new RestBulkStreamingAction(settings));
        registerHandler.accept(new RestUpdateAction());

        registerHandler.accept(new RestSearchAction());
        registerHandler.accept(new RestSearchScrollAction());
        registerHandler.accept(new RestClearScrollAction());
        registerHandler.accept(new RestMultiSearchAction(settings));

        registerHandler.accept(new RestValidateQueryAction());

        registerHandler.accept(new RestExplainAction());

        registerHandler.accept(new RestRecoveryAction());

        registerHandler.accept(new RestReloadSecureSettingsAction());

        // Scripts API
        registerHandler.accept(new RestGetStoredScriptAction());
        registerHandler.accept(new RestPutStoredScriptAction());
        registerHandler.accept(new RestDeleteStoredScriptAction());
        registerHandler.accept(new RestGetScriptContextAction());
        registerHandler.accept(new RestGetScriptLanguageAction());

        registerHandler.accept(new RestFieldCapabilitiesAction());

        // Tasks API
        registerHandler.accept(new RestListTasksAction(nodesInCluster));
        registerHandler.accept(new RestGetTaskAction());
        registerHandler.accept(new RestCancelTasksAction(nodesInCluster));

        // Ingest API
        registerHandler.accept(new RestPutPipelineAction());
        registerHandler.accept(new RestGetPipelineAction());
        registerHandler.accept(new RestDeletePipelineAction());
        registerHandler.accept(new RestSimulatePipelineAction());

        // Dangling indices API
        registerHandler.accept(new RestListDanglingIndicesAction());
        registerHandler.accept(new RestImportDanglingIndexAction());
        registerHandler.accept(new RestDeleteDanglingIndexAction());

        // Data Stream API
        registerHandler.accept(new RestCreateDataStreamAction());
        registerHandler.accept(new RestDeleteDataStreamAction());
        registerHandler.accept(new RestGetDataStreamsAction());
        registerHandler.accept(new RestResolveIndexAction());
        registerHandler.accept(new RestDataStreamsStatsAction());

        // View API
        registerHandler.accept(new RestViewAction.CreateViewHandler());
        registerHandler.accept(new RestViewAction.DeleteViewHandler());
        registerHandler.accept(new RestViewAction.GetViewHandler());
        registerHandler.accept(new RestViewAction.UpdateViewHandler());
        registerHandler.accept(new RestViewAction.SearchViewHandler());
        registerHandler.accept(new RestViewAction.ListViewNamesHandler());

        // CAT API
        registerHandler.accept(new RestAllocationAction());
        registerHandler.accept(new RestCatSegmentReplicationAction());
        registerHandler.accept(new RestShardsAction());
        registerHandler.accept(new RestClusterManagerAction());
        registerHandler.accept(new RestNodesAction());
        registerHandler.accept(new RestTasksAction(nodesInCluster));
        registerHandler.accept(new RestIndicesAction(responseLimitSettings));
        registerHandler.accept(new RestSegmentsAction(responseLimitSettings));
        // Fully qualified to prevent interference with rest.action.count.RestCountAction
        registerHandler.accept(new org.density.rest.action.cat.RestCountAction());
        // Fully qualified to prevent interference with rest.action.indices.RestRecoveryAction
        registerHandler.accept(new RestCatRecoveryAction());
        registerHandler.accept(new RestHealthAction());
        registerHandler.accept(new org.density.rest.action.cat.RestPendingClusterTasksAction());
        registerHandler.accept(new RestAliasAction());
        registerHandler.accept(new RestThreadPoolAction());
        registerHandler.accept(new RestPluginsAction());
        registerHandler.accept(new RestFielddataAction());
        registerHandler.accept(new RestNodeAttrsAction());
        registerHandler.accept(new RestRepositoriesAction());
        registerHandler.accept(new RestSnapshotAction());
        registerHandler.accept(new RestTemplatesAction());

        // LIST API
        registerHandler.accept(new RestIndicesListAction(responseLimitSettings));
        registerHandler.accept(new RestShardsListAction());

        // Point in time API
        registerHandler.accept(new RestCreatePitAction());
        registerHandler.accept(new RestDeletePitAction());
        registerHandler.accept(new RestGetAllPitsAction(nodesInCluster));
        registerHandler.accept(new RestPitSegmentsAction(nodesInCluster));
        registerHandler.accept(new RestDeleteDecommissionStateAction());

        // Search pipelines API
        registerHandler.accept(new RestPutSearchPipelineAction());
        registerHandler.accept(new RestGetSearchPipelineAction());
        registerHandler.accept(new RestDeleteSearchPipelineAction());

        // Extensions API
        if (FeatureFlags.isEnabled(FeatureFlags.EXTENSIONS)) {
            registerHandler.accept(new RestInitializeExtensionAction(extensionsManager));
        }

        for (ActionPlugin plugin : actionPlugins) {
            for (RestHandler handler : plugin.getRestHandlers(
                settings,
                restController,
                clusterSettings,
                indexScopedSettings,
                settingsFilter,
                indexNameExpressionResolver,
                nodesInCluster
            )) {
                registerHandler.accept(handler);
            }
        }
        registerHandler.accept(new RestCatAction(catActions));
        registerHandler.accept(new RestListAction(listActions));
        registerHandler.accept(new RestDecommissionAction());
        registerHandler.accept(new RestGetDecommissionStateAction());
        registerHandler.accept(new RestRemoteStoreStatsAction());
        registerHandler.accept(new RestRestoreRemoteStoreAction());
        registerHandler.accept(new RestRemoteStoreMetadataAction());

        // pull-based ingestion API
        registerHandler.accept(new RestPauseIngestionAction());
        registerHandler.accept(new RestResumeIngestionAction());
        registerHandler.accept(new RestGetIngestionStateAction());
    }

    @Override
    protected void configure() {
        bind(ActionFilters.class).toInstance(actionFilters);
        bind(DestructiveOperations.class).toInstance(destructiveOperations);
        bind(new TypeLiteral<RequestValidators<PutMappingRequest>>() {
        }).toInstance(mappingRequestValidators);
        bind(new TypeLiteral<RequestValidators<IndicesAliasesRequest>>() {
        }).toInstance(indicesAliasesRequestRequestValidators);

        // Supporting classes
        bind(AutoCreateIndex.class).toInstance(autoCreateIndex);
        bind(TransportLivenessAction.class).asEagerSingleton();

        // register ActionType -> transportAction Map used by NodeClient
        @SuppressWarnings("rawtypes")
        MapBinder<ActionType, TransportAction> transportActionsBinder = MapBinder.newMapBinder(
            binder(),
            ActionType.class,
            TransportAction.class
        );
        for (ActionHandler<?, ?> action : actions.values()) {
            // bind the action as eager singleton, so the map binder one will reuse it
            bind(action.getTransportAction()).asEagerSingleton();
            transportActionsBinder.addBinding(action.getAction()).to(action.getTransportAction()).asEagerSingleton();
            for (Class<?> supportAction : action.getSupportTransportActions()) {
                bind(supportAction).asEagerSingleton();
            }
        }

        // register dynamic ActionType -> transportAction Map used by NodeClient
        bind(DynamicActionRegistry.class).toInstance(dynamicActionRegistry);

        bind(ResponseLimitSettings.class).toInstance(responseLimitSettings);
    }

    public ActionFilters getActionFilters() {
        return actionFilters;
    }

    public DynamicActionRegistry getDynamicActionRegistry() {
        return dynamicActionRegistry;
    }

    public RestController getRestController() {
        return restController;
    }

    /**
     * The DynamicActionRegistry maintains a registry mapping {@link ActionType} instances to {@link TransportAction} instances.
     * <p>
     * This class is modeled after {@link NamedRegistry} but provides both register and unregister capabilities.
     *
     * @density.api
     */
    @PublicApi(since = "2.7.0")
    public static class DynamicActionRegistry {
        // This is the unmodifiable actions map created during node bootstrap, which
        // will continue to link ActionType and TransportAction pairs from core and plugin
        // action handler registration.
        private Map<ActionType, TransportAction> actions = Collections.emptyMap();
        // A dynamic registry to add or remove ActionType / TransportAction pairs
        // at times other than node bootstrap.
        private final Map<ActionType<?>, TransportAction<?, ?>> registry = new ConcurrentHashMap<>();

        // A dynamic registry to add or remove Route / RestSendToExtensionAction pairs
        // at times other than node bootstrap.
        private final Map<NamedRoute, RestSendToExtensionAction> routeRegistry = new ConcurrentHashMap<>();

        private final Set<String> registeredActionNames = new ConcurrentSkipListSet<>();

        /**
         * Register the immutable actions in the registry.
         *
         * @param actions The injected map of {@link ActionType} to {@link TransportAction}
         */
        public void registerUnmodifiableActionMap(Map<ActionType, TransportAction> actions) {
            this.actions = actions;
            for (ActionType action : actions.keySet()) {
                registeredActionNames.add(action.name());
            }
        }

        /**
         * Add a dynamic action to the registry.
         *
         * @param action The action instance to add
         * @param transportAction The corresponding instance of transportAction to execute
         */
        public void registerDynamicAction(ActionType<?> action, TransportAction<?, ?> transportAction) {
            requireNonNull(action, "action is required");
            requireNonNull(transportAction, "transportAction is required");
            if (actions.containsKey(action) || registry.putIfAbsent(action, transportAction) != null) {
                throw new IllegalArgumentException("action [" + action.name() + "] already registered");
            }
            registeredActionNames.add(action.name());
        }

        /**
         * Remove a dynamic action from the registry.
         *
         * @param action The action to remove
         */
        public void unregisterDynamicAction(ActionType<?> action) {
            requireNonNull(action, "action is required");
            if (registry.remove(action) == null) {
                throw new IllegalArgumentException("action [" + action.name() + "] was not registered");
            }
            registeredActionNames.remove(action.name());
        }

        /**
         * Checks to see if an action is registered provided an action name
         *
         * @param actionName The name of the action to check
         */
        public boolean isActionRegistered(String actionName) {
            return registeredActionNames.contains(actionName);
        }

        /**
         * Gets the {@link TransportAction} instance corresponding to the {@link ActionType} instance.
         *
         * @param action The {@link ActionType}.
         * @return the corresponding {@link TransportAction} if it is registered, null otherwise.
         */
        @SuppressWarnings("unchecked")
        public TransportAction<? extends ActionRequest, ? extends ActionResponse> get(ActionType<?> action) {
            if (actions.containsKey(action)) {
                return actions.get(action);
            }
            return registry.get(action);
        }

        /**
         * Adds a dynamic route to the registry.
         *
         * @param route The route instance to add
         * @param action The corresponding instance of RestSendToExtensionAction to execute
         */
        public void registerDynamicRoute(NamedRoute route, RestSendToExtensionAction action) {
            requireNonNull(route, "route is required");
            requireNonNull(action, "action is required");

            String routeName = route.name();
            requireNonNull(routeName, "route name is required");
            if (isActionRegistered(routeName)) {
                throw new IllegalArgumentException("route [" + route + "] already registered");
            }

            Set<String> actionNames = route.actionNames();
            if (!Collections.disjoint(actionNames, registeredActionNames)) {
                Set<String> alreadyRegistered = new HashSet<>(registeredActionNames);
                alreadyRegistered.retainAll(actionNames);
                String acts = String.join(", ", alreadyRegistered);
                throw new IllegalArgumentException(
                    "action" + (alreadyRegistered.size() > 1 ? "s [" : " [") + acts + "] already registered"
                );
            }

            if (routeRegistry.containsKey(route)) {
                throw new IllegalArgumentException("route [" + route + "] already registered");
            }
            routeRegistry.put(route, action);
            registeredActionNames.add(routeName);
            registeredActionNames.addAll(actionNames);
        }

        /**
         * Remove a dynamic route from the registry.
         *
         * @param route The route to remove
         */
        public void unregisterDynamicRoute(NamedRoute route) {
            requireNonNull(route, "route is required");
            if (routeRegistry.remove(route) == null) {
                throw new IllegalArgumentException("action [" + route + "] was not registered");
            }

            registeredActionNames.remove(route.name());
            registeredActionNames.removeAll(route.actionNames());
        }

        /**
         * Gets the {@link RestSendToExtensionAction} instance corresponding to the {@link RestHandler.Route} instance.
         *
         * @param route The {@link RestHandler.Route}.
         * @return the corresponding {@link RestSendToExtensionAction} if it is registered, null otherwise.
         */
        public RestSendToExtensionAction get(RestHandler.Route route) {
            if (route instanceof NamedRoute) {
                return routeRegistry.get((NamedRoute) route);
            }
            // Only NamedRoutes are map keys so any other route is not in the map
            return null;
        }
    }
}
