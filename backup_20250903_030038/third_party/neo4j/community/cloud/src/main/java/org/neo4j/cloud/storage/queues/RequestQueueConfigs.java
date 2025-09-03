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

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.READ_IS_FOR_SAMPLING_FLAG;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pullQueueChunkSize;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pullQueueSlotSize;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pushQueueChunkSize;
import static org.neo4j.cloud.storage.StorageSettingsDeclaration.pushQueueSlotSize;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.util.Preconditions.requirePositive;

import org.neo4j.cloud.storage.StoragePath;
import org.neo4j.cloud.storage.StorageSystemProvider;

/**
 * @param pushConfig the config to use when downloading content in a push fashion.
 * @param pullConfig the config to use when downloading content in a pull fashion.
 */
public record RequestQueueConfigs(QueueConfig pushConfig, QueueConfig pullConfig) {

    /**
     * This field and {@link RequestQueueConfigs#SAMPLING_PULL_QUEUE_CHUNK_SIZE} ensures that at most 2 chunks will be
     * loaded for a 4Mb sampling of CSV data for size estimations
     */
    public static final int SAMPLING_PULL_QUEUE_SIZE = 1;

    public static final int SAMPLING_PULL_QUEUE_CHUNK_SIZE = (int) (mebiBytes(2) + kibiBytes(100));

    public RequestQueueConfigs {
        requireNonNull(pushConfig);
        requireNonNull(pullConfig);
    }

    public static RequestQueueConfigs create(StoragePath path) {
        // adapt the number of queue slots for when sampling is being used (ex. getting headers, estimating sizes in
        // CSV imports, etc.) In this case, only a small amount of the initial content is required so a large queue
        // is unnecessary. In the 'normal' access style, a larger queue would be beneficial to help saturate the
        // network interface and speed up downloads
        return new RequestQueueConfigs(pushQueueConfig(path), pullQueueConfig(path, isForSampling(path)));
    }

    private static boolean isForSampling(StoragePath path) {
        return path.metadata().getOrDefault(READ_IS_FOR_SAMPLING_FLAG, Boolean.FALSE) == Boolean.TRUE;
    }

    private static QueueConfig pushQueueConfig(StoragePath path) {
        final var config = StorageSystemProvider.config(path);
        return new QueueConfig(config.get(pushQueueSlotSize(path)), toIntExact(config.get(pushQueueChunkSize(path))));
    }

    private static QueueConfig pullQueueConfig(StoragePath path, boolean isForSampling) {
        final var config = StorageSystemProvider.config(path);
        if (isForSampling) {
            return new QueueConfig(SAMPLING_PULL_QUEUE_SIZE, SAMPLING_PULL_QUEUE_CHUNK_SIZE);
        } else {
            return new QueueConfig(
                    config.get(pullQueueSlotSize(path)), toIntExact(config.get(pullQueueChunkSize(path))));
        }
    }

    /**
     * @param queueSize the size of the queue that maintains at most <code>queueSize</code> requests concurrently running
     * @param chunkSize the size of the data chunk to be downloaded in each request
     */
    public record QueueConfig(int queueSize, int chunkSize) {
        public QueueConfig {
            requirePositive(queueSize);
            requirePositive(chunkSize);
        }
    }
}
