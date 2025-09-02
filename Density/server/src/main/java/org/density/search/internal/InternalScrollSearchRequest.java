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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.search.internal;

import org.density.action.search.SearchScrollRequest;
import org.density.action.search.SearchShardTask;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.tasks.TaskId;
import org.density.search.Scroll;
import org.density.tasks.Task;
import org.density.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

/**
 * Internal request used during scroll search
 *
 * @density.internal
 */
public class InternalScrollSearchRequest extends TransportRequest {

    private ShardSearchContextId contextId;

    private Scroll scroll;

    public InternalScrollSearchRequest() {}

    public InternalScrollSearchRequest(SearchScrollRequest request, ShardSearchContextId contextId) {
        this.contextId = contextId;
        this.scroll = request.scroll();
    }

    public InternalScrollSearchRequest(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        scroll = in.readOptionalWriteable(Scroll::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
        out.writeOptionalWriteable(scroll);
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public Scroll scroll() {
        return scroll;
    }

    public InternalScrollSearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "id[" + contextId.getId() + "], scroll[" + scroll + "]";
    }

}
