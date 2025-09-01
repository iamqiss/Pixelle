/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.action.ActionRequestValidationException;
import org.density.action.support.clustermanager.ClusterManagerNodeRequest;
import org.density.cluster.metadata.WorkloadGroup;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.XContentParser;
import org.density.wlm.MutableWorkloadGroupFragment;

import java.io.IOException;

/**
 * A request for update WorkloadGroup
 *
 * @density.experimental
 */
public class UpdateWorkloadGroupRequest extends ClusterManagerNodeRequest<UpdateWorkloadGroupRequest> {
    private final String name;
    private final MutableWorkloadGroupFragment mutableWorkloadGroupFragment;

    /**
     * Constructor for UpdateWorkloadGroupRequest
     * @param name - WorkloadGroup name for UpdateWorkloadGroupRequest
     * @param mutableWorkloadGroupFragment - MutableWorkloadGroupFragment for UpdateWorkloadGroupRequest
     */
    UpdateWorkloadGroupRequest(String name, MutableWorkloadGroupFragment mutableWorkloadGroupFragment) {
        this.name = name;
        this.mutableWorkloadGroupFragment = mutableWorkloadGroupFragment;
    }

    /**
     * Constructor for UpdateWorkloadGroupRequest
     * @param in - A {@link StreamInput} object
     */
    UpdateWorkloadGroupRequest(StreamInput in) throws IOException {
        this(in.readString(), new MutableWorkloadGroupFragment(in));
    }

    /**
     * Generate a UpdateWorkloadGroupRequest from XContent
     * @param parser - A {@link XContentParser} object
     * @param name - name of the WorkloadGroup to be updated
     */
    public static UpdateWorkloadGroupRequest fromXContent(XContentParser parser, String name) throws IOException {
        WorkloadGroup.Builder builder = WorkloadGroup.Builder.fromXContent(parser);
        return new UpdateWorkloadGroupRequest(name, builder.getMutableWorkloadGroupFragment());
    }

    @Override
    public ActionRequestValidationException validate() {
        WorkloadGroup.validateName(name);
        return null;
    }

    /**
     * name getter
     */
    public String getName() {
        return name;
    }

    /**
     * mutableWorkloadGroupFragment getter
     */
    public MutableWorkloadGroupFragment getmMutableWorkloadGroupFragment() {
        return mutableWorkloadGroupFragment;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        mutableWorkloadGroupFragment.writeTo(out);
    }
}
