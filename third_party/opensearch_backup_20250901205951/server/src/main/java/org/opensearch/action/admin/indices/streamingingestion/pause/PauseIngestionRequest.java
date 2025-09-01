/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.streamingingestion.pause;

import org.density.action.ActionRequestValidationException;
import org.density.action.IndicesRequest;
import org.density.action.support.IndicesOptions;
import org.density.action.support.clustermanager.AcknowledgedRequest;
import org.density.common.annotation.ExperimentalApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.util.CollectionUtils;

import java.io.IOException;

import static org.density.action.ValidateActions.addValidationError;

/**
 * A request to pause ingestion.
 *
 * @density.experimental
 */
@ExperimentalApi
public class PauseIngestionRequest extends AcknowledgedRequest<PauseIngestionRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public PauseIngestionRequest(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readStringArray();
        this.indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    public PauseIngestionRequest(String[] indices) {
        this.indices = indices;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index is missing", validationException);
        }
        return validationException;
    }

    /**
     * Returns the indices to be paused.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    /**
     * Sets the indices to be paused.
     */
    @Override
    public PauseIngestionRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal with wildcard expressions.
     * For example indices that don't exist.
     *
     * @return the desired behaviour regarding indices to ignore and wildcard indices expressions
     */
    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal wild wildcard expressions.
     * For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices to ignore and wildcard indices expressions
     * @return the request itself
     */
    public PauseIngestionRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }
}
