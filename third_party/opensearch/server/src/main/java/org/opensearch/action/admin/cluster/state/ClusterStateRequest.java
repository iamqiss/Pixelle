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

package org.density.action.admin.cluster.state;

import org.density.action.ActionRequestValidationException;
import org.density.action.IndicesRequest;
import org.density.action.support.IndicesOptions;
import org.density.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.density.common.annotation.PublicApi;
import org.density.common.unit.TimeValue;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.tasks.TaskId;
import org.density.rest.action.admin.cluster.ClusterAdminTask;
import org.density.tasks.Task;

import java.io.IOException;
import java.util.Map;

/**
 * Transport request for obtaining cluster state
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStateRequest extends ClusterManagerNodeReadRequest<ClusterStateRequest> implements IndicesRequest.Replaceable {

    public static final TimeValue DEFAULT_WAIT_FOR_NODE_TIMEOUT = TimeValue.timeValueMinutes(1);

    private boolean routingTable = true;
    private boolean nodes = true;
    private boolean metadata = true;
    private boolean blocks = true;
    private boolean customs = true;
    private Long waitForMetadataVersion;
    private TimeValue waitForTimeout = DEFAULT_WAIT_FOR_NODE_TIMEOUT;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();

    public ClusterStateRequest() {}

    public ClusterStateRequest(StreamInput in) throws IOException {
        super(in);
        routingTable = in.readBoolean();
        nodes = in.readBoolean();
        metadata = in.readBoolean();
        blocks = in.readBoolean();
        customs = in.readBoolean();
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        waitForTimeout = in.readTimeValue();
        waitForMetadataVersion = in.readOptionalLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(routingTable);
        out.writeBoolean(nodes);
        out.writeBoolean(metadata);
        out.writeBoolean(blocks);
        out.writeBoolean(customs);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeTimeValue(waitForTimeout);
        out.writeOptionalLong(waitForMetadataVersion);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ClusterStateRequest all() {
        routingTable = true;
        nodes = true;
        metadata = true;
        blocks = true;
        customs = true;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public ClusterStateRequest clear() {
        routingTable = false;
        nodes = false;
        metadata = false;
        blocks = false;
        customs = false;
        indices = Strings.EMPTY_ARRAY;
        return this;
    }

    public boolean routingTable() {
        return routingTable;
    }

    public ClusterStateRequest routingTable(boolean routingTable) {
        this.routingTable = routingTable;
        return this;
    }

    public boolean nodes() {
        return nodes;
    }

    public ClusterStateRequest nodes(boolean nodes) {
        this.nodes = nodes;
        return this;
    }

    public boolean metadata() {
        return metadata;
    }

    public ClusterStateRequest metadata(boolean metadata) {
        this.metadata = metadata;
        return this;
    }

    public boolean blocks() {
        return blocks;
    }

    public ClusterStateRequest blocks(boolean blocks) {
        this.blocks = blocks;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public ClusterStateRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    public final ClusterStateRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public ClusterStateRequest customs(boolean customs) {
        this.customs = customs;
        return this;
    }

    public boolean customs() {
        return customs;
    }

    public TimeValue waitForTimeout() {
        return waitForTimeout;
    }

    public ClusterStateRequest waitForTimeout(TimeValue waitForTimeout) {
        this.waitForTimeout = waitForTimeout;
        return this;
    }

    public Long waitForMetadataVersion() {
        return waitForMetadataVersion;
    }

    public ClusterStateRequest waitForMetadataVersion(long waitForMetadataVersion) {
        if (waitForMetadataVersion < 1) {
            throw new IllegalArgumentException(
                "provided waitForMetadataVersion should be >= 1, but instead is [" + waitForMetadataVersion + "]"
            );
        }
        this.waitForMetadataVersion = waitForMetadataVersion;
        return this;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        if (this.getShouldCancelOnTimeout()) {
            return new ClusterAdminTask(id, type, action, parentTaskId, headers);
        } else {
            return super.createTask(id, type, action, parentTaskId, headers);
        }
    }
}
