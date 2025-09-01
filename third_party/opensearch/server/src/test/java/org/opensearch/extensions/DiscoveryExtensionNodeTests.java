/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions;

import org.density.DensityException;
import org.density.Version;
import org.density.core.common.transport.TransportAddress;
import org.density.test.DensityTestCase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;

public class DiscoveryExtensionNodeTests extends DensityTestCase {

    public void testExtensionNode() throws UnknownHostException {
        DiscoveryExtensionNode extensionNode = new DiscoveryExtensionNode(
            "firstExtension",
            "extensionUniqueId1",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
            new HashMap<String, String>(),
            Version.fromString("3.0.0"),
            // minimum compatible version
            Version.fromString("3.0.0"),
            Collections.emptyList()
        );
        assertTrue(Version.CURRENT.onOrAfter(extensionNode.getMinimumCompatibleVersion()));
    }

    public void testIncompatibleExtensionNode() throws UnknownHostException {
        expectThrows(
            DensityException.class,
            () -> new DiscoveryExtensionNode(
                "firstExtension",
                "extensionUniqueId1",
                new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
                new HashMap<String, String>(),
                Version.fromString("3.0.0"),
                // minimum compatible version
                Version.fromString("3.99.0"),
                Collections.emptyList()
            )
        );
    }
}
