/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.density.ExceptionsHelper;
import org.density.action.support.ActionFilters;
import org.density.action.support.replication.ReplicationMode;
import org.density.action.support.replication.ReplicationRequest;
import org.density.action.support.replication.ReplicationResponse;
import org.density.action.support.replication.ReplicationTask;
import org.density.action.support.replication.TransportReplicationAction;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.common.util.concurrent.ThreadContext;
import org.density.common.util.concurrent.ThreadContextAccess;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.Writeable;
import org.density.index.IndexNotFoundException;
import org.density.index.shard.IndexShard;
import org.density.index.shard.IndexShardClosedException;
import org.density.index.shard.ShardNotInPrimaryModeException;
import org.density.indices.IndicesService;
import org.density.indices.replication.common.ReplicationTimer;
import org.density.node.NodeClosedException;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportException;
import org.density.transport.TransportRequest;
import org.density.transport.TransportResponseHandler;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class for publish checkpoint.
 *
 * @density.api
 */

public abstract class AbstractPublishCheckpointAction<
    Request extends ReplicationRequest<Request>,
    ReplicaRequest extends ReplicationRequest<ReplicaRequest>> extends TransportReplicationAction<
        Request,
        ReplicaRequest,
        ReplicationResponse> {

    private final Logger logger;

    public AbstractPublishCheckpointAction(
        Settings settings,
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        Writeable.Reader<ReplicaRequest> replicaRequestReader,
        String threadPoolName,
        Logger logger
    ) {
        super(
            settings,
            actionName,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            requestReader,
            replicaRequestReader,
            threadPoolName
        );
        this.logger = logger;
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    public ReplicationMode getReplicationMode(IndexShard indexShard) {
        if (indexShard.indexSettings().isAssignedOnRemoteNode()) {
            return ReplicationMode.FULL_REPLICATION;
        }
        return super.getReplicationMode(indexShard);
    }

    /**
     * Publish checkpoint request to shard
     */
    final void doPublish(
        IndexShard indexShard,
        ReplicationCheckpoint checkpoint,
        TransportRequest request,
        String action,
        boolean waitForCompletion,
        TimeValue waitTimeout
    ) {
        String primaryAllocationId = indexShard.routingEntry().allocationId().getId();
        long primaryTerm = indexShard.getPendingPrimaryTerm();
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            ThreadContextAccess.doPrivilegedVoid(threadContext::markAsSystemContext);
            final ReplicationTask task = (ReplicationTask) taskManager.register("transport", action, request);
            final ReplicationTimer timer = new ReplicationTimer();
            timer.start();
            CountDownLatch latch = new CountDownLatch(1);
            transportService.sendChildRequest(
                indexShard.recoveryState().getTargetNode(),
                transportPrimaryAction,
                new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
                task,
                transportOptions,
                new TransportResponseHandler<ReplicationResponse>() {
                    @Override
                    public ReplicationResponse read(StreamInput in) throws IOException {
                        return newResponseInstance(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(ReplicationResponse response) {
                        try {
                            timer.stop();
                            logger.debug(
                                () -> new ParameterizedMessage(
                                    "[shardId {}] Completed publishing checkpoint [{}], timing: {}",
                                    indexShard.shardId().getId(),
                                    checkpoint,
                                    timer.time()
                                )
                            );
                            task.setPhase("finished");
                            taskManager.unregister(task);
                        } finally {
                            latch.countDown();
                        }
                    }

                    @Override
                    public void handleException(TransportException e) {
                        try {
                            timer.stop();
                            logger.debug(
                                "[shardId {}] Failed to publish checkpoint [{}], timing: {}",
                                indexShard.shardId().getId(),
                                checkpoint,
                                timer.time()
                            );
                            task.setPhase("finished");
                            taskManager.unregister(task);
                            if (ExceptionsHelper.unwrap(
                                e,
                                NodeClosedException.class,
                                IndexNotFoundException.class,
                                AlreadyClosedException.class,
                                IndexShardClosedException.class,
                                ShardNotInPrimaryModeException.class
                            ) != null) {
                                // Node is shutting down or the index was deleted or the shard is closed
                                return;
                            }
                            logger.warn(
                                new ParameterizedMessage(
                                    "{} segment replication checkpoint [{}] publishing failed",
                                    indexShard.shardId(),
                                    checkpoint
                                ),
                                e
                            );
                        } finally {
                            latch.countDown();
                        }
                    }
                }
            );
            logger.trace(
                () -> new ParameterizedMessage(
                    "[shardId {}] Publishing replication checkpoint [{}]",
                    checkpoint.getShardId().getId(),
                    checkpoint
                )
            );
            if (waitForCompletion) {
                try {
                    latch.await(waitTimeout.seconds(), TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.warn(
                        () -> new ParameterizedMessage("Interrupted while waiting for publish checkpoint complete [{}]", checkpoint),
                        e
                    );
                }
            }
        }
    }

    @Override
    final protected void shardOperationOnReplica(ReplicaRequest shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        Objects.requireNonNull(shardRequest);
        Objects.requireNonNull(replica);
        ActionListener.completeWith(listener, () -> {
            logger.trace(() -> new ParameterizedMessage("Checkpoint {} received on replica {}", shardRequest, replica.shardId()));
            // Condition for ensuring that we ignore Segrep checkpoints received on Docrep shard copies.
            // This case will hit iff the replica hosting node is not remote enabled and replication type != SEGMENT
            if (replica.indexSettings().isAssignedOnRemoteNode() == false && replica.indexSettings().isSegRepLocalEnabled() == false) {
                logger.trace("Received segrep checkpoint on a docrep shard copy during an ongoing remote migration. NoOp.");
                return new ReplicaResult();
            }
            doReplicaOperation(shardRequest, replica);
            return new ReplicaResult();
        });
    }

    /**
     * Execute the specified replica operation.
     *
     * @param shardRequest the request to the replica shard
     * @param replica      the replica shard to perform the operation on
     */
    protected abstract void doReplicaOperation(ReplicaRequest shardRequest, IndexShard replica);
}
