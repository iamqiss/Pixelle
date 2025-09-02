/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.action.ActionRequestValidationException;
import org.density.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.density.cluster.metadata.WorkloadGroup;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Request for get WorkloadGroup
 *
 * @density.experimental
 */
public class GetWorkloadGroupRequest extends ClusterManagerNodeReadRequest<GetWorkloadGroupRequest> {
    final String name;

    /**
     * Default constructor for GetWorkloadGroupRequest
     * @param name - name for the WorkloadGroup to get
     */
    public GetWorkloadGroupRequest(String name) {
        this.name = name;
    }

    /**
     * Constructor for GetWorkloadGroupRequest
     * @param in - A {@link StreamInput} object
     */
    public GetWorkloadGroupRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readOptionalString();
    }

    @Override
    public ActionRequestValidationException validate() {
        if (name != null) {
            WorkloadGroup.validateName(name);
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
    }

    /**
     * Name getter
     */
    public String getName() {
        return name;
    }
}
