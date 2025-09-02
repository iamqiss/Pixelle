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
import org.density.test.DensityTestCase;

public class OpTypeProtoUtilsTests extends DensityTestCase {

    public void testFromProtoWithOpTypeCreate() {
        // Test conversion from OpType.OP_TYPE_CREATE to DocWriteRequest.OpType.CREATE
        DocWriteRequest.OpType result = OpTypeProtoUtils.fromProto(OpType.OP_TYPE_CREATE);

        // Verify the result
        assertEquals("OP_TYPE_CREATE should convert to DocWriteRequest.OpType.CREATE", DocWriteRequest.OpType.CREATE, result);
    }

    public void testFromProtoWithOpTypeIndex() {
        // Test conversion from OpType.OP_TYPE_INDEX to DocWriteRequest.OpType.INDEX
        DocWriteRequest.OpType result = OpTypeProtoUtils.fromProto(OpType.OP_TYPE_INDEX);

        // Verify the result
        assertEquals("OP_TYPE_INDEX should convert to DocWriteRequest.OpType.INDEX", DocWriteRequest.OpType.INDEX, result);
    }

    public void testFromProtoWithOpTypeUnspecified() {
        // Test conversion from OpType.OP_TYPE_UNSPECIFIED, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> OpTypeProtoUtils.fromProto(OpType.OP_TYPE_UNSPECIFIED)
        );

        // Verify the exception message
        assertTrue("Exception message should mention 'Invalid optype'", exception.getMessage().contains("Invalid optype"));
    }

    public void testFromProtoWithUnrecognizedOpType() {
        // Test conversion with an unrecognized OpType, should throw UnsupportedOperationException
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> OpTypeProtoUtils.fromProto(OpType.UNRECOGNIZED)
        );

        // Verify the exception message
        assertTrue("Exception message should mention 'Invalid optype'", exception.getMessage().contains("Invalid optype"));
    }
}
