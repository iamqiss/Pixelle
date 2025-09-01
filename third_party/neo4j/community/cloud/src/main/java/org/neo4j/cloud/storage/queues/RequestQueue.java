/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cloud.storage.queues;

import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.requireNonNegative;
import static org.neo4j.util.Preconditions.requirePositive;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.neo4j.cloud.storage.queues.RequestQueueConfigs.QueueConfig;

/**
 * Utility for processing as many cloud range requests as sensibly possible. A queue of requests is maintained and
 * an attempt is made to keep it as full as possible with requests for a contiguous range of chunks.
 */
public abstract class RequestQueue implements AutoCloseable {

    private final Queue<CompletableFuture<ByteBuffer>> queue;

    private final QueueConfig queueConfig;
    private final long objectSize;

    private long nextRequestPosition;

    /**
     * @param queueConfig the queue config for sizing and chunk size
     * @param objectSize the total size of the object
     * @param startPosition the initial position of the object
     */
    protected RequestQueue(QueueConfig queueConfig, long objectSize, long startPosition) {
        this.queueConfig = requireNonNull(queueConfig);
        this.objectSize = requirePositive(objectSize);
        this.nextRequestPosition = requireNonNegative(startPosition);
        this.queue = new ArrayDeque<>(queueConfig.queueSize());
    }

    /**
     * @param from the start of the byte range (inclusive)
     * @param to the end of the byte range (exclusive)
     * @return the data as specified by the range header
     */
    protected abstract CompletableFuture<ByteBuffer> get(long from, long to);

    @Override
    public void close() {
        clearQueue();
    }

    protected final QueueConfig queueConfig() {
        return queueConfig;
    }

    protected final long getObjectSize() {
        return objectSize;
    }

    protected final int queueSize() {
        return queue.size();
    }

    protected final long nextRequestPosition() {
        return nextRequestPosition;
    }

    protected final void setNextRequestPosition(long nextRequestPosition) {
        this.nextRequestPosition = requireNonNegative(nextRequestPosition);
    }

    protected final void fillQueue() {
        for (var i = queueSize(); i < queueConfig().queueSize(); i++) {
            if (!maybeRequestChunk()) {
                break;
            }
        }
    }

    protected final CompletableFuture<ByteBuffer> poll(boolean preloadNextRequests) {
        final var next = queue.poll();
        if (next != null && preloadNextRequests) {
            fillQueue();
        }
        return next;
    }

    protected final void clearQueue() {
        for (var response : queue) {
            response.cancel(true);
        }
        queue.clear();
    }

    protected final boolean maybeRequestChunk() {
        final var rangeStart = nextRequestPosition;
        if (rangeStart >= objectSize) {
            // all potential requests are in flight or have already been handled
            return false;
        }

        nextRequestPosition += queueConfig.chunkSize();
        queue.offer(request(rangeStart));
        return true;
    }

    protected void onBeforeLoad(long from, long to) {
        // do what thou whilst
    }

    private CompletableFuture<ByteBuffer> request(long from) {
        final var end = Math.min(from + queueConfig.chunkSize(), objectSize);
        onBeforeLoad(from, end);
        return get(from, end);
    }
}
