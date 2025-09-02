/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication.checkpoint;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.action.support.ActionFilters;
import org.density.action.support.replication.ReplicationResponse;
import org.density.cluster.action.shard.ShardStateAction;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.common.util.UploadListener;
import org.density.core.action.ActionListener;
import org.density.index.shard.IndexShard;
import org.density.index.shard.RemoteStoreUploader;
import org.density.index.shard.RemoteStoreUploaderService;
import org.density.indices.IndicesService;
import org.density.indices.replication.SegmentReplicationTargetService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Replication action responsible for uploading merged segment to a remote store and
 * publishing {@link RemoteStoreMergedSegmentCheckpoint} to all replica shards.
 *
 * @density.api
 */
public class RemoteStorePublishMergedSegmentAction extends AbstractPublishCheckpointAction<
    RemoteStorePublishMergedSegmentRequest,
    RemoteStorePublishMergedSegmentRequest> implements MergedSegmentPublisher.PublishAction {

    public static final String ACTION_NAME = "indices:admin/remote_publish_merged_segment";

    private final static Logger logger = LogManager.getLogger(RemoteStorePublishMergedSegmentAction.class);

    private final SegmentReplicationTargetService replicationService;

    @Inject
    public RemoteStorePublishMergedSegmentAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        SegmentReplicationTargetService targetService
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            RemoteStorePublishMergedSegmentRequest::new,
            RemoteStorePublishMergedSegmentRequest::new,
            ThreadPool.Names.GENERIC,
            logger
        );
        this.replicationService = targetService;
    }

    @Override
    protected void doReplicaOperation(RemoteStorePublishMergedSegmentRequest shardRequest, IndexShard replica) {
        RemoteStoreMergedSegmentCheckpoint checkpoint = shardRequest.getMergedSegment();
        if (checkpoint.getShardId().equals(replica.shardId())) {
            replica.getRemoteDirectory().markMergedSegmentsPendingDownload(checkpoint.getLocalToRemoteSegmentFilenameMap());
            replicationService.onNewMergedSegmentCheckpoint(checkpoint, replica);
        } else {
            logger.warn(
                () -> new ParameterizedMessage(
                    "Received merged segment checkpoint for shard {} on replica shard {}, ignoring checkpoint",
                    checkpoint.getShardId(),
                    replica.shardId()
                )
            );
        }
    }

    @Override
    protected void shardOperationOnPrimary(
        RemoteStorePublishMergedSegmentRequest shardRequest,
        IndexShard primary,
        ActionListener<PrimaryResult<RemoteStorePublishMergedSegmentRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> new PrimaryResult<>(shardRequest, new ReplicationResponse()));
    }

    @Override
    public final void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
        long startTimeMillis = System.currentTimeMillis();
        Map<String, String> localToRemoteStoreFilenames = uploadMergedSegmentsToRemoteStore(indexShard, checkpoint);
        long endTimeMillis = System.currentTimeMillis();

        long elapsedTimeMillis = endTimeMillis - startTimeMillis;
        long timeoutMillis = indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().millis();
        long timeLeftMillis = Math.max(0, timeoutMillis - elapsedTimeMillis);

        if (timeLeftMillis > 0) {
            RemoteStoreMergedSegmentCheckpoint remoteStoreMergedSegmentCheckpoint = new RemoteStoreMergedSegmentCheckpoint(
                checkpoint,
                localToRemoteStoreFilenames
            );
            doPublish(
                indexShard,
                remoteStoreMergedSegmentCheckpoint,
                new RemoteStorePublishMergedSegmentRequest(remoteStoreMergedSegmentCheckpoint),
                "segrep_remote_publish_merged_segment",
                true,
                TimeValue.timeValueMillis(timeLeftMillis)
            );
        } else {
            logger.warn(
                () -> new ParameterizedMessage(
                    "Unable to confirm upload of merged segment {} to remote store. Timeout of {}ms exceeded. Skipping pre-copy.",
                    checkpoint,
                    TimeValue.timeValueMillis(elapsedTimeMillis).toHumanReadableString(3)
                )
            );
        }
    }

    private Map<String, String> uploadMergedSegmentsToRemoteStore(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
        Collection<String> segmentsToUpload = checkpoint.getMetadataMap().keySet();
        Map<String, String> localToRemoteStoreFilenames = new ConcurrentHashMap<>();

        Map<String, Long> segmentsSizeMap = checkpoint.getMetadataMap()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().length()));
        final CountDownLatch latch = new CountDownLatch(1);
        getRemoteStoreUploaderService(indexShard).uploadSegments(segmentsToUpload, segmentsSizeMap, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.trace(() -> new ParameterizedMessage("Successfully uploaded segments {} to remote store", segmentsToUpload));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("Failed to upload segments {} to remote store. {}", segmentsToUpload, e));
                latch.countDown();
            }
        }, (x) -> new UploadListener() {
            @Override
            public void beforeUpload(String file) {}

            @Override
            public void onSuccess(String file) {
                localToRemoteStoreFilenames.put(file, indexShard.getRemoteDirectory().getExistingRemoteFilename(file));
            }

            @Override
            public void onFailure(String file) {
                logger.warn("Unable to upload segments during merge. Continuing.");
            }
        }, true);
        try {
            long timeout = indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().seconds();
            if (latch.await(timeout, TimeUnit.SECONDS) == false) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Timeout exceeded {}s: Could not verify merge segment downloads were completed by replicas. Continuing.",
                        timeout
                    )
                );
            }
        } catch (InterruptedException e) {
            logger.warn(
                () -> new ParameterizedMessage(
                    "Unable to confirm successful merge segment downloads by replicas due to interruption. Continuing. \nException - {}",
                    e
                )
            );
        }

        return localToRemoteStoreFilenames;
    }

    private RemoteStoreUploader getRemoteStoreUploaderService(IndexShard indexShard) {
        return new RemoteStoreUploaderService(indexShard, indexShard.store().directory(), indexShard.getRemoteDirectory());
    }
}
