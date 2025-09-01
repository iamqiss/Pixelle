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

package org.density.action.search;

import org.density.action.ActionRequestBuilder;
import org.density.common.annotation.PublicApi;
import org.density.transport.client.DensityClient;

import java.util.List;

/**
 * Transport request builder for clearing a search scroll
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class ClearScrollRequestBuilder extends ActionRequestBuilder<ClearScrollRequest, ClearScrollResponse> {

    public ClearScrollRequestBuilder(DensityClient client, ClearScrollAction action) {
        super(client, action, new ClearScrollRequest());
    }

    public ClearScrollRequestBuilder setScrollIds(List<String> cursorIds) {
        request.setScrollIds(cursorIds);
        return this;
    }

    public ClearScrollRequestBuilder addScrollId(String cursorId) {
        request.addScrollId(cursorId);
        return this;
    }
}
