/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.ActionRequestValidationException;
import org.density.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.density.common.annotation.PublicApi;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * Request to get search pipelines
 *
 * @density.api
 */
@PublicApi(since = "2.7.0")
public class GetSearchPipelineRequest extends ClusterManagerNodeReadRequest<GetSearchPipelineRequest> {
    private final String[] ids;

    public GetSearchPipelineRequest(String... ids) {
        this.ids = Objects.requireNonNull(ids);
    }

    public GetSearchPipelineRequest() {
        ids = Strings.EMPTY_ARRAY;
    }

    public GetSearchPipelineRequest(StreamInput in) throws IOException {
        super(in);
        ids = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(ids);
    }

    public String[] getIds() {
        return ids;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
