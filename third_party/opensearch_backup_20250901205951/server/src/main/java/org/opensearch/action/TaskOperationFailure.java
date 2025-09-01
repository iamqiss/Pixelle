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

package org.density.action;

import org.density.ExceptionsHelper;
import org.density.DensityException;
import org.density.core.ParseField;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.ConstructingObjectParser;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;

import java.io.IOException;

import static org.density.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Information about task operation failures
 * <p>
 * The class is final due to serialization limitations
 *
 * @density.internal
 */
public final class TaskOperationFailure implements Writeable, ToXContentFragment {
    private static final String TASK_ID = "task_id";
    private static final String NODE_ID = "node_id";
    private static final String STATUS = "status";
    private static final String REASON = "reason";
    private final String nodeId;

    private final long taskId;

    private final Exception reason;

    private final RestStatus status;

    private static final ConstructingObjectParser<TaskOperationFailure, Void> PARSER = new ConstructingObjectParser<>(
        "task_info",
        true,
        constructorObjects -> {
            int i = 0;
            String nodeId = (String) constructorObjects[i++];
            long taskId = (long) constructorObjects[i++];
            DensityException reason = (DensityException) constructorObjects[i];
            return new TaskOperationFailure(nodeId, taskId, reason);
        }
    );

    static {
        PARSER.declareString(constructorArg(), new ParseField(NODE_ID));
        PARSER.declareLong(constructorArg(), new ParseField(TASK_ID));
        PARSER.declareObject(constructorArg(), (parser, c) -> DensityException.fromXContent(parser), new ParseField(REASON));
    }

    public TaskOperationFailure(String nodeId, long taskId, Exception e) {
        this.nodeId = nodeId;
        this.taskId = taskId;
        this.reason = e;
        status = ExceptionsHelper.status(e);
    }

    /**
     * Read from a stream.
     */
    public TaskOperationFailure(StreamInput in) throws IOException {
        nodeId = in.readString();
        taskId = in.readLong();
        reason = in.readException();
        status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeLong(taskId);
        out.writeException(reason);
        RestStatus.writeTo(out, status);
    }

    public String getNodeId() {
        return this.nodeId;
    }

    public long getTaskId() {
        return this.taskId;
    }

    public RestStatus getStatus() {
        return status;
    }

    public Exception getCause() {
        return reason;
    }

    @Override
    public String toString() {
        return "[" + nodeId + "][" + taskId + "] failed, reason [" + reason + "]";
    }

    public static TaskOperationFailure fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TASK_ID, getTaskId());
        builder.field(NODE_ID, getNodeId());
        builder.field(STATUS, status.name());
        if (reason != null) {
            builder.field(REASON);
            builder.startObject();
            DensityException.generateThrowableXContent(builder, params, reason);
            builder.endObject();
        }
        return builder;

    }
}
