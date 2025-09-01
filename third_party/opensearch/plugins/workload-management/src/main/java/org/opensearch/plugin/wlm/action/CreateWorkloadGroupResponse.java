/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.cluster.metadata.WorkloadGroup;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.ToXContentObject;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for the create API for WorkloadGroup
 *
 * @density.experimental
 */
public class CreateWorkloadGroupResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final WorkloadGroup workloadGroup;
    private final RestStatus restStatus;

    /**
     * Constructor for CreateWorkloadGroupResponse
     * @param workloadGroup - The WorkloadGroup to be included in the response
     * @param restStatus - The restStatus for the response
     */
    public CreateWorkloadGroupResponse(final WorkloadGroup workloadGroup, RestStatus restStatus) {
        this.workloadGroup = workloadGroup;
        this.restStatus = restStatus;
    }

    /**
     * Constructor for CreateWorkloadGroupResponse
     * @param in - A {@link StreamInput} object
     */
    public CreateWorkloadGroupResponse(StreamInput in) throws IOException {
        workloadGroup = new WorkloadGroup(in);
        restStatus = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        workloadGroup.writeTo(out);
        RestStatus.writeTo(out, restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return workloadGroup.toXContent(builder, params);
    }

    /**
     * workloadGroup getter
     */
    public WorkloadGroup getWorkloadGroup() {
        return workloadGroup;
    }

    /**
     * restStatus getter
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }
}
