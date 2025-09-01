/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.response.exceptions;

import org.density.action.FailedNodeException;
import org.density.protobufs.ObjectMap;
import org.density.test.DensityTestCase;

import java.util.Map;

public class FailedNodeExceptionProtoUtilsTests extends DensityTestCase {

    public void testMetadataToProto() {
        // Create a FailedNodeException with a specific node ID
        String nodeId = "test_node_id";
        FailedNodeException exception = new FailedNodeException(nodeId, "Test failed node", new RuntimeException("Cause"));

        // Convert to Protocol Buffer
        Map<String, ObjectMap.Value> metadata = FailedNodeExceptionProtoUtils.metadataToProto(exception);

        // Verify the conversion
        assertTrue("Should have node_id field", metadata.containsKey("node_id"));

        // Verify field value
        ObjectMap.Value nodeIdValue = metadata.get("node_id");
        assertEquals("node_id should match", nodeId, nodeIdValue.getString());
    }
}
