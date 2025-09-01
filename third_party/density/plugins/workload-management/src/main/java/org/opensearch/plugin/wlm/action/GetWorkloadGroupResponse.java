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
import java.util.Collection;

/**
 * Response for the get API for WorkloadGroup
 *
 * @density.experimental
 */
public class GetWorkloadGroupResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final Collection<WorkloadGroup> workloadGroups;
    private final RestStatus restStatus;

    /**
     * Constructor for GetWorkloadGroupResponse
     * @param workloadGroups - The WorkloadGroup list to be fetched
     * @param restStatus - The rest status of the request
     */
    public GetWorkloadGroupResponse(final Collection<WorkloadGroup> workloadGroups, RestStatus restStatus) {
        this.workloadGroups = workloadGroups;
        this.restStatus = restStatus;
    }

    /**
     * Constructor for GetWorkloadGroupResponse
     * @param in - A {@link StreamInput} object
     */
    public GetWorkloadGroupResponse(StreamInput in) throws IOException {
        this.workloadGroups = in.readList(WorkloadGroup::new);
        restStatus = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(workloadGroups);
        RestStatus.writeTo(out, restStatus);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("workload_groups");
        for (WorkloadGroup group : workloadGroups) {
            group.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * workloadGroups getter
     */
    public Collection<WorkloadGroup> getWorkloadGroups() {
        return workloadGroups;
    }

    /**
     * restStatus getter
     */
    public RestStatus getRestStatus() {
        return restStatus;
    }
}
