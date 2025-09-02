/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.state;

import org.density.action.admin.indices.streamingingestion.IngestionStateShardFailure;
import org.density.action.support.broadcast.BroadcastResponse;
import org.density.common.annotation.ExperimentalApi;
import org.density.core.action.support.DefaultShardOperationFailedException;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * Transport response for updating ingestion state.
 *
 * @density.experimental
 */
@ExperimentalApi
public class UpdateIngestionStateResponse extends BroadcastResponse {

    private boolean acknowledged;
    private String errorMessage = "";
    private final IngestionStateShardFailure[] shardFailureList;

    public UpdateIngestionStateResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
        errorMessage = in.readString();
        shardFailureList = in.readArray(IngestionStateShardFailure::new, IngestionStateShardFailure[]::new);
    }

    public UpdateIngestionStateResponse(
        boolean acknowledged,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.acknowledged = acknowledged;
        this.shardFailureList = shardFailures.stream()
            .map(shardFailure -> new IngestionStateShardFailure(shardFailure.index(), shardFailure.shardId(), shardFailure.reason()))
            .toArray(IngestionStateShardFailure[]::new);

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(acknowledged);
        out.writeString(errorMessage);
        out.writeArray(shardFailureList);
    }

    public IngestionStateShardFailure[] getShardFailureList() {
        return shardFailureList;
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
