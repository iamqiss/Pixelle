/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density;

import org.density.transport.client.transport.NoNodeAvailableException;

import static org.density.DensityException.DensityExceptionHandle;
import static org.density.DensityException.DensityExceptionHandleRegistry.registerExceptionHandle;
import static org.density.DensityException.UNKNOWN_VERSION_ADDED;
import static org.density.Version.V_2_10_0;
import static org.density.Version.V_2_13_0;
import static org.density.Version.V_2_17_0;
import static org.density.Version.V_2_18_0;
import static org.density.Version.V_2_1_0;
import static org.density.Version.V_2_4_0;
import static org.density.Version.V_2_5_0;
import static org.density.Version.V_2_6_0;
import static org.density.Version.V_2_7_0;
import static org.density.Version.V_3_0_0;
import static org.density.Version.V_3_2_0;

/**
 * Utility class to register server exceptions
 *
 * @density.internal
 */
public final class DensityServerException {

    private DensityServerException() {
        // no ctor:
    }

    /**
     * Setting a higher base exception id to avoid conflicts.
     */
    private static final int CUSTOM_ELASTICSEARCH_EXCEPTIONS_BASE_ID = 10000;

    public static void registerExceptions() {
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.dfs.DfsPhaseExecutionException.class,
                org.density.search.dfs.DfsPhaseExecutionException::new,
                1,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.common.util.CancellableThreads.ExecutionCancelledException.class,
                org.density.common.util.CancellableThreads.ExecutionCancelledException::new,
                2,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.discovery.ClusterManagerNotDiscoveredException.class,
                org.density.discovery.ClusterManagerNotDiscoveredException::new,
                3,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.DensitySecurityException.class,
                org.density.DensitySecurityException::new,
                4,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.snapshots.IndexShardRestoreException.class,
                org.density.index.snapshots.IndexShardRestoreException::new,
                5,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.IndexClosedException.class,
                org.density.indices.IndexClosedException::new,
                6,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.http.BindHttpException.class,
                org.density.http.BindHttpException::new,
                7,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.search.ReduceSearchPhaseException.class,
                org.density.action.search.ReduceSearchPhaseException::new,
                8,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.node.NodeClosedException.class,
                org.density.node.NodeClosedException::new,
                9,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.SnapshotFailedEngineException.class,
                org.density.index.engine.SnapshotFailedEngineException::new,
                10,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.ShardNotFoundException.class,
                org.density.index.shard.ShardNotFoundException::new,
                11,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.ConnectTransportException.class,
                org.density.transport.ConnectTransportException::new,
                12,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.NotSerializableTransportException.class,
                org.density.transport.NotSerializableTransportException::new,
                13,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.ResponseHandlerFailureTransportException.class,
                org.density.transport.ResponseHandlerFailureTransportException::new,
                14,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.IndexCreationException.class,
                org.density.indices.IndexCreationException::new,
                15,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.IndexNotFoundException.class,
                org.density.index.IndexNotFoundException::new,
                16,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.routing.IllegalShardRoutingStateException.class,
                org.density.cluster.routing.IllegalShardRoutingStateException::new,
                17,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.support.broadcast.BroadcastShardOperationFailedException.class,
                org.density.action.support.broadcast.BroadcastShardOperationFailedException::new,
                18,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.ResourceNotFoundException.class,
                org.density.ResourceNotFoundException::new,
                19,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.ActionTransportException.class,
                org.density.transport.ActionTransportException::new,
                20,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.DensityGenerationException.class,
                org.density.DensityGenerationException::new,
                21,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 22 was CreateFailedEngineException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.IndexShardStartedException.class,
                org.density.index.shard.IndexShardStartedException::new,
                23,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.SearchContextMissingException.class,
                org.density.search.SearchContextMissingException::new,
                24,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.script.GeneralScriptException.class,
                org.density.script.GeneralScriptException::new,
                25,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 26 was BatchOperationException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.snapshots.SnapshotCreationException.class,
                org.density.snapshots.SnapshotCreationException::new,
                27,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 28 was DeleteFailedEngineException, deprecated in 6.0, removed in 7.0
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.DocumentMissingException.class,
                org.density.index.engine.DocumentMissingException::new,
                29,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.snapshots.SnapshotException.class,
                org.density.snapshots.SnapshotException::new,
                30,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.InvalidAliasNameException.class,
                org.density.indices.InvalidAliasNameException::new,
                31,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.InvalidIndexNameException.class,
                org.density.indices.InvalidIndexNameException::new,
                32,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.IndexPrimaryShardNotAllocatedException.class,
                org.density.indices.IndexPrimaryShardNotAllocatedException::new,
                33,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.TransportException.class,
                org.density.transport.TransportException::new,
                34,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.SearchException.class,
                org.density.search.SearchException::new,
                36,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.mapper.MapperException.class,
                org.density.index.mapper.MapperException::new,
                37,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.InvalidTypeNameException.class,
                org.density.indices.InvalidTypeNameException::new,
                38,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.snapshots.SnapshotRestoreException.class,
                org.density.snapshots.SnapshotRestoreException::new,
                39,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.IndexShardClosedException.class,
                org.density.index.shard.IndexShardClosedException::new,
                41,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.recovery.RecoverFilesRecoveryException.class,
                org.density.indices.recovery.RecoverFilesRecoveryException::new,
                42,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.translog.TruncatedTranslogException.class,
                org.density.index.translog.TruncatedTranslogException::new,
                43,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.recovery.RecoveryFailedException.class,
                org.density.indices.recovery.RecoveryFailedException::new,
                44,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.IndexShardRelocatedException.class,
                org.density.index.shard.IndexShardRelocatedException::new,
                45,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.NodeShouldNotConnectException.class,
                org.density.transport.NodeShouldNotConnectException::new,
                46,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 47 used to be for IndexTemplateAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.translog.TranslogCorruptedException.class,
                org.density.index.translog.TranslogCorruptedException::new,
                48,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.block.ClusterBlockException.class,
                org.density.cluster.block.ClusterBlockException::new,
                49,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.fetch.FetchPhaseExecutionException.class,
                org.density.search.fetch.FetchPhaseExecutionException::new,
                50,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 51 used to be for IndexShardAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.VersionConflictEngineException.class,
                org.density.index.engine.VersionConflictEngineException::new,
                52,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.EngineException.class,
                org.density.index.engine.EngineException::new,
                53,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 54 was DocumentAlreadyExistsException, which is superseded by VersionConflictEngineException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.NoSuchNodeException.class,
                org.density.action.NoSuchNodeException::new,
                55,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.common.settings.SettingsException.class,
                org.density.common.settings.SettingsException::new,
                56,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.IndexTemplateMissingException.class,
                org.density.indices.IndexTemplateMissingException::new,
                57,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.SendRequestTransportException.class,
                org.density.transport.SendRequestTransportException::new,
                58,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 59 used to be DensityRejectedExecutionException
        // 60 used to be for EarlyTerminationException
        // 61 used to be for RoutingValidationException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.AliasFilterParsingException.class,
                org.density.indices.AliasFilterParsingException::new,
                63,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 64 was DeleteByQueryFailedEngineException, which was removed in 5.0
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.gateway.GatewayException.class,
                org.density.gateway.GatewayException::new,
                65,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.IndexShardNotRecoveringException.class,
                org.density.index.shard.IndexShardNotRecoveringException::new,
                66,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.http.HttpException.class,
                org.density.http.HttpException::new,
                67,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.DensityException.class,
                org.density.DensityException::new,
                68,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.snapshots.SnapshotMissingException.class,
                org.density.snapshots.SnapshotMissingException::new,
                69,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.PrimaryMissingActionException.class,
                org.density.action.PrimaryMissingActionException::new,
                70,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.SearchParseException.class,
                org.density.search.SearchParseException::new,
                72,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.FailedNodeException.class,
                org.density.action.FailedNodeException::new,
                71,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.snapshots.ConcurrentSnapshotExecutionException.class,
                org.density.snapshots.ConcurrentSnapshotExecutionException::new,
                73,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.common.blobstore.BlobStoreException.class,
                org.density.common.blobstore.BlobStoreException::new,
                74,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.IncompatibleClusterStateVersionException.class,
                org.density.cluster.IncompatibleClusterStateVersionException::new,
                75,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.RecoveryEngineException.class,
                org.density.index.engine.RecoveryEngineException::new,
                76,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.common.util.concurrent.UncategorizedExecutionException.class,
                org.density.common.util.concurrent.UncategorizedExecutionException::new,
                77,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.TimestampParsingException.class,
                org.density.action.TimestampParsingException::new,
                78,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.RoutingMissingException.class,
                org.density.action.RoutingMissingException::new,
                79,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 80 was IndexFailedEngineException, deprecated in 6.0, removed in 7.0
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.snapshots.IndexShardRestoreFailedException.class,
                org.density.index.snapshots.IndexShardRestoreFailedException::new,
                81,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.repositories.RepositoryException.class,
                org.density.repositories.RepositoryException::new,
                82,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.ReceiveTimeoutTransportException.class,
                org.density.transport.ReceiveTimeoutTransportException::new,
                83,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.NodeDisconnectedException.class,
                org.density.transport.NodeDisconnectedException::new,
                84,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 85 used to be for AlreadyExpiredException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.aggregations.AggregationExecutionException.class,
                org.density.search.aggregations.AggregationExecutionException::new,
                86,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 87 used to be for MergeMappingException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.InvalidIndexTemplateException.class,
                org.density.indices.InvalidIndexTemplateException::new,
                88,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.RefreshFailedEngineException.class,
                org.density.index.engine.RefreshFailedEngineException::new,
                90,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.aggregations.AggregationInitializationException.class,
                org.density.search.aggregations.AggregationInitializationException::new,
                91,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.recovery.DelayRecoveryException.class,
                org.density.indices.recovery.DelayRecoveryException::new,
                92,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 93 used to be for IndexWarmerMissingException
        registerExceptionHandle(
            new DensityExceptionHandle(NoNodeAvailableException.class, NoNodeAvailableException::new, 94, UNKNOWN_VERSION_ADDED)
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.snapshots.InvalidSnapshotNameException.class,
                org.density.snapshots.InvalidSnapshotNameException::new,
                96,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.IllegalIndexShardStateException.class,
                org.density.index.shard.IllegalIndexShardStateException::new,
                97,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.core.index.snapshots.IndexShardSnapshotException.class,
                org.density.core.index.snapshots.IndexShardSnapshotException::new,
                98,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.IndexShardNotStartedException.class,
                org.density.index.shard.IndexShardNotStartedException::new,
                99,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.search.SearchPhaseExecutionException.class,
                org.density.action.search.SearchPhaseExecutionException::new,
                100,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.ActionNotFoundTransportException.class,
                org.density.transport.ActionNotFoundTransportException::new,
                101,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.TransportSerializationException.class,
                org.density.transport.TransportSerializationException::new,
                102,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.RemoteTransportException.class,
                org.density.transport.RemoteTransportException::new,
                103,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.EngineCreationFailureException.class,
                org.density.index.engine.EngineCreationFailureException::new,
                104,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.routing.RoutingException.class,
                org.density.cluster.routing.RoutingException::new,
                105,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.IndexShardRecoveryException.class,
                org.density.index.shard.IndexShardRecoveryException::new,
                106,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.repositories.RepositoryMissingException.class,
                org.density.repositories.RepositoryMissingException::new,
                107,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.DocumentSourceMissingException.class,
                org.density.index.engine.DocumentSourceMissingException::new,
                109,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 110 used to be FlushNotAllowedEngineException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.common.settings.NoClassSettingsException.class,
                org.density.common.settings.NoClassSettingsException::new,
                111,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.BindTransportException.class,
                org.density.transport.BindTransportException::new,
                112,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.rest.action.admin.indices.AliasesNotFoundException.class,
                org.density.rest.action.admin.indices.AliasesNotFoundException::new,
                113,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.IndexShardRecoveringException.class,
                org.density.index.shard.IndexShardRecoveringException::new,
                114,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.translog.TranslogException.class,
                org.density.index.translog.TranslogException::new,
                115,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.metadata.ProcessClusterEventTimeoutException.class,
                org.density.cluster.metadata.ProcessClusterEventTimeoutException::new,
                116,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.support.replication.ReplicationOperation.RetryOnPrimaryException.class,
                org.density.action.support.replication.ReplicationOperation.RetryOnPrimaryException::new,
                117,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.DensityTimeoutException.class,
                org.density.DensityTimeoutException::new,
                118,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.query.QueryPhaseExecutionException.class,
                org.density.search.query.QueryPhaseExecutionException::new,
                119,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.repositories.RepositoryVerificationException.class,
                org.density.repositories.RepositoryVerificationException::new,
                120,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.aggregations.InvalidAggregationPathException.class,
                org.density.search.aggregations.InvalidAggregationPathException::new,
                121,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 123 used to be IndexAlreadyExistsException and was renamed
        registerExceptionHandle(
            new DensityExceptionHandle(
                ResourceAlreadyExistsException.class,
                ResourceAlreadyExistsException::new,
                123,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 124 used to be Script.ScriptParseException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.TcpTransport.HttpRequestOnTransportException.class,
                org.density.transport.TcpTransport.HttpRequestOnTransportException::new,
                125,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.mapper.MapperParsingException.class,
                org.density.index.mapper.MapperParsingException::new,
                126,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 127 used to be org.density.search.SearchContextException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.builder.SearchSourceBuilderException.class,
                org.density.search.builder.SearchSourceBuilderException::new,
                128,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 129 was EngineClosedException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.NoShardAvailableActionException.class,
                org.density.action.NoShardAvailableActionException::new,
                130,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.UnavailableShardsException.class,
                org.density.action.UnavailableShardsException::new,
                131,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.FlushFailedEngineException.class,
                org.density.index.engine.FlushFailedEngineException::new,
                132,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.NodeNotConnectedException.class,
                org.density.transport.NodeNotConnectedException::new,
                134,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.mapper.StrictDynamicMappingException.class,
                org.density.index.mapper.StrictDynamicMappingException::new,
                135,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class,
                org.density.action.support.replication.TransportReplicationAction.RetryOnReplicaException::new,
                136,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.TypeMissingException.class,
                org.density.indices.TypeMissingException::new,
                137,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.coordination.FailedToCommitClusterStateException.class,
                org.density.cluster.coordination.FailedToCommitClusterStateException::new,
                140,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.query.QueryShardException.class,
                org.density.index.query.QueryShardException::new,
                141,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.action.shard.ShardStateAction.NoLongerPrimaryShardException.class,
                org.density.cluster.action.shard.ShardStateAction.NoLongerPrimaryShardException::new,
                142,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.script.ScriptException.class,
                org.density.script.ScriptException::new,
                143,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.NotClusterManagerException.class,
                org.density.cluster.NotClusterManagerException::new,
                144,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.DensityStatusException.class,
                org.density.DensityStatusException::new,
                145,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.env.ShardLockObtainFailedException.class,
                org.density.env.ShardLockObtainFailedException::new,
                147,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 148 was UnknownNamedObjectException
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.aggregations.MultiBucketConsumerService.TooManyBucketsException.class,
                org.density.search.aggregations.MultiBucketConsumerService.TooManyBucketsException::new,
                149,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.coordination.CoordinationStateRejectedException.class,
                org.density.cluster.coordination.CoordinationStateRejectedException::new,
                150,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.snapshots.SnapshotInProgressException.class,
                org.density.snapshots.SnapshotInProgressException::new,
                151,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.NoSuchRemoteClusterException.class,
                org.density.transport.NoSuchRemoteClusterException::new,
                152,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.seqno.RetentionLeaseAlreadyExistsException.class,
                org.density.index.seqno.RetentionLeaseAlreadyExistsException::new,
                153,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.seqno.RetentionLeaseNotFoundException.class,
                org.density.index.seqno.RetentionLeaseNotFoundException::new,
                154,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.ShardNotInPrimaryModeException.class,
                org.density.index.shard.ShardNotInPrimaryModeException::new,
                155,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.seqno.RetentionLeaseInvalidRetainingSeqNoException.class,
                org.density.index.seqno.RetentionLeaseInvalidRetainingSeqNoException::new,
                156,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.ingest.IngestProcessorException.class,
                org.density.ingest.IngestProcessorException::new,
                157,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.recovery.PeerRecoveryNotFound.class,
                org.density.indices.recovery.PeerRecoveryNotFound::new,
                158,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.coordination.NodeHealthCheckFailureException.class,
                org.density.cluster.coordination.NodeHealthCheckFailureException::new,
                159,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.NoSeedNodeLeftException.class,
                org.density.transport.NoSeedNodeLeftException::new,
                160,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.replication.common.ReplicationFailedException.class,
                org.density.indices.replication.common.ReplicationFailedException::new,
                161,
                V_2_1_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.shard.PrimaryShardClosedException.class,
                org.density.index.shard.PrimaryShardClosedException::new,
                162,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.decommission.DecommissioningFailedException.class,
                org.density.cluster.decommission.DecommissioningFailedException::new,
                163,
                V_2_4_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.decommission.NodeDecommissionedException.class,
                org.density.cluster.decommission.NodeDecommissionedException::new,
                164,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.service.ClusterManagerThrottlingException.class,
                org.density.cluster.service.ClusterManagerThrottlingException::new,
                165,
                Version.V_2_5_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.snapshots.SnapshotInUseDeletionException.class,
                org.density.snapshots.SnapshotInUseDeletionException::new,
                166,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.routing.UnsupportedWeightedRoutingStateException.class,
                org.density.cluster.routing.UnsupportedWeightedRoutingStateException::new,
                167,
                V_2_5_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.routing.PreferenceBasedSearchNotAllowedException.class,
                org.density.cluster.routing.PreferenceBasedSearchNotAllowedException::new,
                168,
                V_2_6_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.routing.NodeWeighedAwayException.class,
                org.density.cluster.routing.NodeWeighedAwayException::new,
                169,
                V_2_6_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.search.pipeline.SearchPipelineProcessingException.class,
                org.density.search.pipeline.SearchPipelineProcessingException::new,
                170,
                V_2_7_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.crypto.CryptoRegistryException.class,
                org.density.crypto.CryptoRegistryException::new,
                171,
                V_2_10_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.admin.indices.view.ViewNotFoundException.class,
                org.density.action.admin.indices.view.ViewNotFoundException::new,
                172,
                V_2_13_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.action.admin.indices.view.ViewAlreadyExistsException.class,
                org.density.action.admin.indices.view.ViewAlreadyExistsException::new,
                173,
                V_2_13_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.indices.InvalidIndexContextException.class,
                org.density.indices.InvalidIndexContextException::new,
                174,
                V_2_17_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.common.breaker.ResponseLimitBreachedException.class,
                org.density.common.breaker.ResponseLimitBreachedException::new,
                175,
                V_2_18_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.cluster.block.IndexCreateBlockException.class,
                org.density.cluster.block.IndexCreateBlockException::new,
                CUSTOM_ELASTICSEARCH_EXCEPTIONS_BASE_ID + 1,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.index.engine.IngestionEngineException.class,
                org.density.index.engine.IngestionEngineException::new,
                176,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new DensityExceptionHandle(
                org.density.transport.stream.StreamException.class,
                org.density.transport.stream.StreamException::new,
                177,
                V_3_2_0
            )
        );
    }
}
