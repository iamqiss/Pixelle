/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.discovery;

import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.BytesStreamInput;
import org.density.core.common.transport.TransportAddress;
import org.density.extensions.DiscoveryExtensionNode;
import org.density.extensions.ExtensionDependency;
import org.density.test.DensityTestCase;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

public class InitializeExtensionRequestTests extends DensityTestCase {

    public void testInitializeExtensionRequest() throws Exception {
        String expectedUniqueId = "test uniqueid";
        Version expectedVersion = Version.fromString("2.0.0");
        String expectedServiceAccountHeader = "test";
        ExtensionDependency expectedDependency = new ExtensionDependency(expectedUniqueId, expectedVersion);
        DiscoveryExtensionNode expectedExtensionNode = new DiscoveryExtensionNode(
            "firstExtension",
            "uniqueid1",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
            new HashMap<>(),
            Version.CURRENT,
            Version.CURRENT,
            List.of(expectedDependency)
        );
        DiscoveryNode expectedSourceNode = new DiscoveryNode(
            "sourceNode",
            "uniqueid2",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 1000),
            new HashMap<>(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        InitializeExtensionRequest initializeExtensionRequest = new InitializeExtensionRequest(
            expectedSourceNode,
            expectedExtensionNode,
            expectedServiceAccountHeader
        );
        assertEquals(expectedExtensionNode, initializeExtensionRequest.getExtension());
        assertEquals(expectedSourceNode, initializeExtensionRequest.getSourceNode());
        assertEquals(expectedServiceAccountHeader, initializeExtensionRequest.getServiceAccountHeader());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            initializeExtensionRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                initializeExtensionRequest = new InitializeExtensionRequest(in);

                assertEquals(expectedExtensionNode, initializeExtensionRequest.getExtension());
                assertEquals(expectedSourceNode, initializeExtensionRequest.getSourceNode());
                assertEquals(expectedServiceAccountHeader, initializeExtensionRequest.getServiceAccountHeader());
            }
        }
    }
}
