/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Generic boolean response indicating the status of some previous request sent to the SDK
 *
 * @density.internal
 */
public class AcknowledgedResponse extends TransportResponse {

    private final boolean status;

    /**
     * @param status Boolean indicating the status of the parse request sent to the SDK
     */
    public AcknowledgedResponse(boolean status) {
        this.status = status;
    }

    public AcknowledgedResponse(StreamInput in) throws IOException {
        super(in);
        this.status = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(status);
    }

    @Override
    public String toString() {
        return "AcknowledgedResponse{" + "status=" + this.status + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AcknowledgedResponse that = (AcknowledgedResponse) o;
        return Objects.equals(this.status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status);
    }

    /**
     * Returns a boolean indicating the success of the request sent to the SDK
     */
    public boolean getStatus() {
        return this.status;
    }

}
