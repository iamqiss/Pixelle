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

package org.density.action.admin.cluster.node.tasks.cancel;

import org.density.DensityException;
import org.density.action.TaskOperationFailure;
import org.density.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.density.common.annotation.PublicApi;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.xcontent.ConstructingObjectParser;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;
import org.density.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;

/**
 * Returns the list of tasks that were cancelled
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class CancelTasksResponse extends ListTasksResponse {

    private static final ConstructingObjectParser<CancelTasksResponse, Void> PARSER = setupParser(
        "cancel_tasks_response",
        CancelTasksResponse::new
    );

    public CancelTasksResponse(StreamInput in) throws IOException {
        super(in);
    }

    public CancelTasksResponse(
        List<TaskInfo> tasks,
        List<TaskOperationFailure> taskFailures,
        List<? extends DensityException> nodeFailures
    ) {
        super(tasks, taskFailures, nodeFailures);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return super.toXContent(builder, params);
    }

    public static CancelTasksResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
