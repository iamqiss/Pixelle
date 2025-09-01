/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.ActionRequestValidationException;
import org.density.action.support.clustermanager.AcknowledgedRequest;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to delete a search pipeline
 *
 * @density.api
 */
@PublicApi(since = "2.7.0")
public class DeleteSearchPipelineRequest extends AcknowledgedRequest<DeleteSearchPipelineRequest> {
    private String id;

    public DeleteSearchPipelineRequest(String id) {
        this.id = Objects.requireNonNull(id);
    }

    public DeleteSearchPipelineRequest() {}

    public DeleteSearchPipelineRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
