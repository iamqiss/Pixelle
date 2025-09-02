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

package org.density.action.admin.indices.alias.get;

import org.density.action.ActionType;
import org.density.action.support.IndicesOptions;
import org.density.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.density.common.util.ArrayUtils;
import org.density.core.action.ActionResponse;
import org.density.transport.client.DensityClient;

/**
 * Base request builder for listing index aliases
 *
 * @density.internal
 */
public abstract class BaseAliasesRequestBuilder<
    Response extends ActionResponse,
    Builder extends BaseAliasesRequestBuilder<Response, Builder>> extends ClusterManagerNodeReadOperationRequestBuilder<
        GetAliasesRequest,
        Response,
        Builder> {

    public BaseAliasesRequestBuilder(DensityClient client, ActionType<Response> action, String... aliases) {
        super(client, action, new GetAliasesRequest(aliases));
    }

    @SuppressWarnings("unchecked")
    public Builder setAliases(String... aliases) {
        request.aliases(aliases);
        return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder addAliases(String... aliases) {
        request.aliases(ArrayUtils.concat(request.aliases(), aliases));
        return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder setIndices(String... indices) {
        request.indices(indices);
        return (Builder) this;
    }

    @SuppressWarnings("unchecked")
    public Builder addIndices(String... indices) {
        request.indices(ArrayUtils.concat(request.indices(), indices));
        return (Builder) this;
    }

    /**
     * Specifies what type of requested indices to ignore and wildcard indices expressions.
     * <p>
     * For example indices that don't exist.
     */
    @SuppressWarnings("unchecked")
    public Builder setIndicesOptions(IndicesOptions options) {
        request.indicesOptions(options);
        return (Builder) this;
    }

}
