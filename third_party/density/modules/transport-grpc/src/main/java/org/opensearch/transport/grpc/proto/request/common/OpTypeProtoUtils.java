/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.request.common;

import org.density.action.DocWriteRequest;
import org.density.protobufs.OpType;

/**
 * Utility class for converting OpType Protocol Buffers to Density DocWriteRequest.OpType objects.
 * This class handles the conversion of Protocol Buffer representations to their
 * corresponding Density operation type enumerations.
 */
public class OpTypeProtoUtils {

    private OpTypeProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer OpType to its corresponding Density DocWriteRequest.OpType.
     * Similar to {@link DocWriteRequest.OpType}.
     *
     * @param opType The Protocol Buffer OpType to convert
     * @return The corresponding Density DocWriteRequest.OpType
     * @throws UnsupportedOperationException if the operation type is not supported
     */
    public static DocWriteRequest.OpType fromProto(OpType opType) {

        switch (opType) {
            case OP_TYPE_CREATE:
                return DocWriteRequest.OpType.CREATE;
            case OP_TYPE_INDEX:
                return DocWriteRequest.OpType.INDEX;
            default:
                throw new UnsupportedOperationException("Invalid optype: " + opType);
        }
    }
}
