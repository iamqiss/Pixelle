
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.transport.TransportRequest;

import java.io.IOException;

/**
 * Inner node get all pits request
 */
public class GetAllPitNodeRequest extends TransportRequest {

    public GetAllPitNodeRequest() {
        super();
    }

    public GetAllPitNodeRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }
}
