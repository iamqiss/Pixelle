/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * Request from Density to an Extension
 *
 * @density.internal
 */
public class DensityRequest extends TransportRequest {

    private static final Logger logger = LogManager.getLogger(DensityRequest.class);
    private ExtensionsManager.DensityRequestType requestType;

    /**
     * @param requestType String identifying the default extension point to invoke on the extension
     */
    public DensityRequest(ExtensionsManager.DensityRequestType requestType) {
        this.requestType = requestType;
    }

    /**
     * @param in StreamInput from which a string identifying the default extension point to invoke on the extension is read from
     */
    public DensityRequest(StreamInput in) throws IOException {
        super(in);
        this.requestType = in.readEnum(ExtensionsManager.DensityRequestType.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeEnum(requestType);
    }

    @Override
    public String toString() {
        return "DensityRequest{" + "requestType=" + requestType + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DensityRequest that = (DensityRequest) o;
        return Objects.equals(requestType, that.requestType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestType);
    }

    public ExtensionsManager.DensityRequestType getRequestType() {
        return this.requestType;
    }

}
