/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.state;

import org.density.action.ActionRequestValidationException;
import org.density.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.density.action.support.broadcast.BroadcastRequest;
import org.density.common.Nullable;
import org.density.common.annotation.ExperimentalApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.density.action.ValidateActions.addValidationError;

/**
 * Holds metadata required for updating ingestion state.
 *
 * <p> This is for internal use only and will not be exposed to the user. </p>
 *
 * @density.experimental
 */
@ExperimentalApi
public class UpdateIngestionStateRequest extends BroadcastRequest<UpdateIngestionStateRequest> {
    private String[] index;
    private int[] shards;

    // Following will be optional parameters and will be used to decide when to update shard ingestion state if non-null values are provided
    @Nullable
    private Boolean ingestionPaused;

    // Optional reset settings to be applied before resuming ingestion.
    @Nullable
    private ResumeIngestionRequest.ResetSettings[] resetSettings;

    public UpdateIngestionStateRequest(String[] index, int[] shards) {
        super();
        this.index = index;
        this.shards = shards;
    }

    public UpdateIngestionStateRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readStringArray();
        this.shards = in.readVIntArray();
        this.ingestionPaused = in.readOptionalBoolean();
        this.resetSettings = in.readOptionalArray(ResumeIngestionRequest.ResetSettings::new, ResumeIngestionRequest.ResetSettings[]::new);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (index == null) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(index);
        out.writeVIntArray(shards);
        out.writeOptionalBoolean(ingestionPaused);
        out.writeOptionalArray(resetSettings);
    }

    public String[] getIndex() {
        return index;
    }

    public int[] getShards() {
        return shards;
    }

    public void setShards(int[] shards) {
        this.shards = shards;
    }

    public Boolean getIngestionPaused() {
        return ingestionPaused;
    }

    public void setIngestionPaused(boolean ingestionPaused) {
        this.ingestionPaused = ingestionPaused;
    }

    @Nullable
    public ResumeIngestionRequest.ResetSettings[] getResetSettings() {
        return resetSettings;
    }

    public void setResetSettings(ResumeIngestionRequest.ResetSettings[] resetSettings) {
        this.resetSettings = resetSettings;
    }
}
