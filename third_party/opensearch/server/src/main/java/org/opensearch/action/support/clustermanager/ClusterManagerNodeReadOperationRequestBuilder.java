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

package org.density.action.support.clustermanager;

import org.density.action.ActionType;
import org.density.core.action.ActionResponse;
import org.density.transport.client.DensityClient;

/**
 * Base request builder for cluster-manager node read operations that can be executed on the local node as well
 *
 * @density.internal
 */
public abstract class ClusterManagerNodeReadOperationRequestBuilder<
    Request extends ClusterManagerNodeReadRequest<Request>,
    Response extends ActionResponse,
    RequestBuilder extends ClusterManagerNodeReadOperationRequestBuilder<Request, Response, RequestBuilder>> extends
    ClusterManagerNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

    protected ClusterManagerNodeReadOperationRequestBuilder(DensityClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
    }

    /**
     * Specifies if the request should be executed on local node rather than on master
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setLocal(boolean local) {
        request.local(local);
        return (RequestBuilder) this;
    }
}
