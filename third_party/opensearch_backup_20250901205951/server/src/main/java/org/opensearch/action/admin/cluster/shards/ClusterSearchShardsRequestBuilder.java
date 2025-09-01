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

package org.density.action.admin.cluster.shards;

import org.density.action.support.IndicesOptions;
import org.density.action.support.clustermanager.ClusterManagerNodeReadOperationRequestBuilder;
import org.density.common.annotation.PublicApi;
import org.density.transport.client.DensityClient;

/**
 * Transport request builder for searching shards
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class ClusterSearchShardsRequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<
    ClusterSearchShardsRequest,
    ClusterSearchShardsResponse,
    ClusterSearchShardsRequestBuilder> {

    public ClusterSearchShardsRequestBuilder(DensityClient client, ClusterSearchShardsAction action) {
        super(client, action, new ClusterSearchShardsRequest());
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public ClusterSearchShardsRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public ClusterSearchShardsRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public ClusterSearchShardsRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * {@code _local} to prefer local shards, {@code _primary} to execute only on primary shards,
     * or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public ClusterSearchShardsRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Specifies what type of requested indices to ignore and how to deal indices wildcard expressions.
     * For example indices that don't exist.
     */
    public ClusterSearchShardsRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }
}
