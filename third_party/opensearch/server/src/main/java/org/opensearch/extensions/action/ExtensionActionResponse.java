/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.action;

import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * This class encapsulates the transport response from extension
 *
 * @density.internal
 */
public class ExtensionActionResponse extends ActionResponse {
    /**
     * responseBytes is the raw bytes being transported between extensions.
     */
    private byte[] responseBytes;

    /**
     * ExtensionActionResponse constructor.
     *
     * @param responseBytes is the raw bytes being transported between extensions.
     */
    public ExtensionActionResponse(byte[] responseBytes) {
        this.responseBytes = responseBytes;
    }

    /**
     * ExtensionActionResponse constructor from {@link StreamInput}.
     *
     * @param in bytes stream input used to de-serialize the message.
     * @throws IOException when message de-serialization fails.
     */
    public ExtensionActionResponse(StreamInput in) throws IOException {
        responseBytes = in.readByteArray();
    }

    public byte[] getResponseBytes() {
        return responseBytes;
    }

    public void setResponseBytes(byte[] responseBytes) {
        this.responseBytes = responseBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(responseBytes);
    }
}
