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
package org.neo4j.cloud.storage;

import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;

import java.util.Objects;
import org.neo4j.cloud.storage.queues.PullQueue;
import org.neo4j.cloud.storage.queues.PushQueue;
import org.neo4j.configuration.SettingBuilder;
import org.neo4j.configuration.SettingConstraint;
import org.neo4j.configuration.SettingValueParser;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

public abstract class StorageSettingsDeclaration implements SettingsDeclaration {

    public static final long DOWNLOAD_CHUNK_SIZE = mebiBytes(8);

    public static final String READ_IS_FOR_SAMPLING_FLAG = "cloud.storage.read.sampling";

    private static final SettingConstraint<Long> CHUNK_RANGE = range(mebiBytes(1), gibiBytes(1));

    /**
     * Adapts a {@link StoragePath} so that any future partial reads of its data (i.e. for sampling) can use an optimised
     * @param path the path to adapt
     * @return a new {@link StoragePath} that has the {@link #READ_IS_FOR_SAMPLING_FLAG} set to <code>true</code>
     */
    public static StoragePath adaptPathForSampling(StoragePath path) {
        return path.copy().addMetadata(READ_IS_FOR_SAMPLING_FLAG, Boolean.TRUE);
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate push queue slot size setting (based on the {@link StoragePath} scheme
     */
    public static Setting<Integer> pushQueueSlotSize(StoragePath path) {
        return pushQueueSlotSize(scheme(path));
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate push queue chunk size setting (based on the {@link StoragePath} scheme
     */
    public static Setting<Long> pushQueueChunkSize(StoragePath path) {
        return pushQueueChunkSize(scheme(path));
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate pull queue slot size setting (based on the {@link StoragePath} scheme
     */
    public static Setting<Integer> pullQueueSlotSize(StoragePath path) {
        return pullQueueSlotSize(scheme(path));
    }

    /**
     * @param path the path to use for determining the setting
     * @return the appropriate pull queue chunk size setting (based on the {@link StoragePath} scheme
     */
    public static Setting<Long> pullQueueChunkSize(StoragePath path) {
        return pullQueueChunkSize(scheme(path));
    }

    protected static Setting<Integer> pushQueueSlotSize(String scheme) {
        return option(scheme, "push", "slot", INT, PushQueue.QUEUE_SIZE)
                .addConstraint(min(16))
                .build();
    }

    protected static Setting<Long> pushQueueChunkSize(String scheme) {
        return option(scheme, "push", "chunk", BYTES, DOWNLOAD_CHUNK_SIZE)
                .addConstraint(CHUNK_RANGE)
                .build();
    }

    protected static Setting<Integer> pullQueueSlotSize(String scheme) {
        return option(scheme, "pull", "slot", INT, PullQueue.QUEUE_SIZE)
                .addConstraint(min(1))
                .build();
    }

    protected static Setting<Long> pullQueueChunkSize(String scheme) {
        return option(scheme, "pull", "chunk", BYTES, DOWNLOAD_CHUNK_SIZE)
                .addConstraint(CHUNK_RANGE)
                .build();
    }

    private static <S> SettingBuilder<S> option(
            String scheme, String queueType, String optionType, SettingValueParser<S> parser, S defaultValue) {
        return newBuilder(
                "internal.dbms.cloud.storage.%s.%s_queue_%s_size".formatted(scheme, queueType, optionType),
                parser,
                defaultValue);
    }

    private static String scheme(StoragePath path) {
        return Objects.requireNonNull(path).scheme();
    }
}
