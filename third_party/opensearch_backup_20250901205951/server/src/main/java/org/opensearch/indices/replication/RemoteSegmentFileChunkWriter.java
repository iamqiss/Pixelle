/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.replication;

import org.apache.lucene.store.RateLimiter;
import org.density.DensityException;
import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.Writeable;
import org.density.core.index.shard.ShardId;
import org.density.core.transport.TransportResponse;
import org.density.index.store.StoreFileMetadata;
import org.density.indices.recovery.FileChunkRequest;
import org.density.indices.recovery.FileChunkWriter;
import org.density.indices.recovery.RecoverySettings;
import org.density.indices.recovery.RetryableTransportClient;
import org.density.transport.TransportRequestOptions;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * This class handles sending file chunks over the transport layer to a target shard.
 *
 * @density.internal
 */
public final class RemoteSegmentFileChunkWriter implements FileChunkWriter {

    private final AtomicLong requestSeqNoGenerator;
    private final RetryableTransportClient retryableTransportClient;
    private final ShardId shardId;
    private final long replicationId;
    private final AtomicLong bytesSinceLastPause = new AtomicLong();
    private final TransportRequestOptions fileChunkRequestOptions;
    private final Consumer<Long> onSourceThrottle;
    private final Supplier<RateLimiter> rateLimiterSupplier;
    private final String action;

    public RemoteSegmentFileChunkWriter(
        long replicationId,
        RecoverySettings recoverySettings,
        RetryableTransportClient retryableTransportClient,
        ShardId shardId,
        String action,
        AtomicLong requestSeqNoGenerator,
        Consumer<Long> onSourceThrottle,
        Supplier<RateLimiter> rateLimiterSupplier
    ) {
        this.replicationId = replicationId;
        this.retryableTransportClient = retryableTransportClient;
        this.shardId = shardId;
        this.requestSeqNoGenerator = requestSeqNoGenerator;
        this.onSourceThrottle = onSourceThrottle;
        this.rateLimiterSupplier = rateLimiterSupplier;
        this.fileChunkRequestOptions = TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.RECOVERY)
            .withTimeout(recoverySettings.internalActionTimeout())
            .build();

        this.action = action;
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        // Pause using the rate limiter, if desired, to throttle the recovery
        final long throttleTimeInNanos;
        // always fetch the ratelimiter - it might be updated in real-time on the recovery settings
        final RateLimiter rl = rateLimiterSupplier.get();
        if (rl != null) {
            long bytes = bytesSinceLastPause.addAndGet(content.length());
            if (bytes > rl.getMinPauseCheckBytes()) {
                // Time to pause
                bytesSinceLastPause.addAndGet(-bytes);
                try {
                    throttleTimeInNanos = rl.pause(bytes);
                    onSourceThrottle.accept(throttleTimeInNanos);
                } catch (IOException e) {
                    throw new DensityException("failed to pause recovery", e);
                }
            } else {
                throttleTimeInNanos = 0;
            }
        } else {
            throttleTimeInNanos = 0;
        }

        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        /* we send estimateTotalOperations with every request since we collect stats on the target and that way we can
         * see how many translog ops we accumulate while copying files across the network. A future optimization
         * would be in to restart file copy again (new deltas) if we have too many translog ops are piling up.
         */
        final FileChunkRequest request = new FileChunkRequest(
            replicationId,
            requestSeqNo,
            shardId,
            fileMetadata,
            position,
            content,
            lastChunk,
            totalTranslogOps,
            throttleTimeInNanos
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        retryableTransportClient.executeRetryableAction(
            action,
            request,
            fileChunkRequestOptions,
            ActionListener.map(listener, r -> null),
            reader
        );
    }

    @Override
    public void cancel() {
        retryableTransportClient.cancel();
    }
}
