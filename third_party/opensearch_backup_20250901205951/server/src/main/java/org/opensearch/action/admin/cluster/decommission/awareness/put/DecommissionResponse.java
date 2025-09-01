/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.put;

import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.ToXContentObject;

import java.io.IOException;

/**
 * Response for decommission request
 *
 * @density.api
 */
@PublicApi(since = "2.4.0")
public class DecommissionResponse extends AcknowledgedResponse implements ToXContentObject {

    public DecommissionResponse(StreamInput in) throws IOException {
        super(in);
    }

    public DecommissionResponse(boolean acknowledged) {
        super(acknowledged);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
