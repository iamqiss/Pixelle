/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion;

import org.density.common.annotation.ExperimentalApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Indicates ingestion failures at index and shard level.
 *
 * @density.experimental
 */
@ExperimentalApi
public record IngestionStateShardFailure(String index, int shard, String errorMessage) implements Writeable, ToXContentFragment {

    private static final String SHARD = "shard";
    private static final String ERROR = "error";

    public IngestionStateShardFailure(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt(), in.readString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shard);
        out.writeString(errorMessage);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SHARD, shard);
        builder.field(ERROR, errorMessage);
        return builder.endObject();
    }

    /**
     * Groups provided shard ingestion state failures by index name.
     */
    public static Map<String, List<IngestionStateShardFailure>> groupShardFailuresByIndex(IngestionStateShardFailure[] shardFailures) {
        Map<String, List<IngestionStateShardFailure>> shardFailuresByIndex = new HashMap<>();

        for (IngestionStateShardFailure shardFailure : shardFailures) {
            shardFailuresByIndex.computeIfAbsent(shardFailure.index(), (index) -> new ArrayList<>());
            shardFailuresByIndex.get(shardFailure.index()).add(shardFailure);
        }

        return shardFailuresByIndex;
    }
}
