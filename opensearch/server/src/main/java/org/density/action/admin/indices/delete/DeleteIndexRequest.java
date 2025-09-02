/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.action.admin.indices.delete;

import org.density.action.ActionRequestValidationException;
import org.density.action.IndicesRequest;
import org.density.action.support.IndicesOptions;
import org.density.action.support.clustermanager.AcknowledgedRequest;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.util.CollectionUtils;
import org.density.transport.client.Requests;

import java.io.IOException;

import static org.density.action.ValidateActions.addValidationError;

/**
 * A request to delete an index. Best created with {@link Requests#deleteIndexRequest(String)}.
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class DeleteIndexRequest extends AcknowledgedRequest<DeleteIndexRequest> implements IndicesRequest.Replaceable {

    private String[] indices;
    // Delete index should work by default on both open and closed indices.
    private IndicesOptions indicesOptions = IndicesOptions.fromOptions(false, true, true, true, false, false, true, false);

    public DeleteIndexRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
    }

    public DeleteIndexRequest() {}

    /**
     * Constructs a new delete index request for the specified index.
     *
     * @param index The index to delete. Use "_all" to delete all indices.
     */
    public DeleteIndexRequest(String index) {
        this.indices = new String[] { index };
    }

    /**
     * Constructs a new delete index request for the specified indices.
     *
     * @param indices The indices to delete. Use "_all" to delete all indices.
     */
    public DeleteIndexRequest(String... indices) {
        this.indices = indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public DeleteIndexRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (CollectionUtils.isEmpty(indices)) {
            validationException = addValidationError("index / indices is missing", validationException);
        }
        return validationException;
    }

    @Override
    public DeleteIndexRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * The index to delete.
     */
    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
    }
}
