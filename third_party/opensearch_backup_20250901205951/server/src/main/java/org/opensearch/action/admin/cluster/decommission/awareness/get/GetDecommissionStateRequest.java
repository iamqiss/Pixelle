/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.get;

import org.density.action.ActionRequestValidationException;
import org.density.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.density.common.annotation.PublicApi;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.density.action.ValidateActions.addValidationError;

/**
 * Get Decommissioned attribute request
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class GetDecommissionStateRequest extends ClusterManagerNodeReadRequest<GetDecommissionStateRequest> {

    private String attributeName;

    public GetDecommissionStateRequest() {}

    /**
     * Constructs a new get decommission state request with given attribute name
     *
     * @param attributeName name of the attribute
     */
    public GetDecommissionStateRequest(String attributeName) {
        this.attributeName = attributeName;
    }

    public GetDecommissionStateRequest(StreamInput in) throws IOException {
        super(in);
        attributeName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(attributeName);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (attributeName == null || Strings.isEmpty(attributeName)) {
            validationException = addValidationError("attribute name is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets attribute name
     *
     * @param attributeName attribute name
     * @return this request
     */
    public GetDecommissionStateRequest attributeName(String attributeName) {
        this.attributeName = attributeName;
        return this;
    }

    /**
     * Returns attribute name
     *
     * @return attributeName name of attribute
     */
    public String attributeName() {
        return this.attributeName;
    }
}
