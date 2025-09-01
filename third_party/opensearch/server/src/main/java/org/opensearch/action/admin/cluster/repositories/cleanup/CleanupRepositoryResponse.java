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

package org.density.action.admin.cluster.repositories.cleanup;

import org.density.common.annotation.PublicApi;
import org.density.core.ParseField;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.ObjectParser;
import org.density.core.xcontent.ToXContentObject;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;
import org.density.repositories.RepositoryCleanupResult;

import java.io.IOException;

/**
 * Transport response for cleaning up snapshot repositories
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public final class CleanupRepositoryResponse extends ActionResponse implements ToXContentObject {

    private static final ObjectParser<CleanupRepositoryResponse, Void> PARSER = new ObjectParser<>(
        CleanupRepositoryResponse.class.getName(),
        true,
        CleanupRepositoryResponse::new
    );

    static {
        PARSER.declareObject(
            (response, cleanupResult) -> response.result = cleanupResult,
            RepositoryCleanupResult.PARSER,
            new ParseField("results")
        );
    }

    private RepositoryCleanupResult result;

    public CleanupRepositoryResponse() {}

    public CleanupRepositoryResponse(RepositoryCleanupResult result) {
        this.result = result;
    }

    public CleanupRepositoryResponse(StreamInput in) throws IOException {
        result = new RepositoryCleanupResult(in);
    }

    public RepositoryCleanupResult result() {
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        result.writeTo(out);
    }

    public static CleanupRepositoryResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("results");
        result.toXContent(builder, params);
        builder.endObject();
        return builder;
    }
}
