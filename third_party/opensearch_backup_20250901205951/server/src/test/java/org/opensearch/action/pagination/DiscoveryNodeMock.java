/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.pagination;

import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.core.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;

public class DiscoveryNodeMock {
    public static DiscoveryNode createDummyNode(int index) {
        try {
            InetAddress address = InetAddress.getByName("127.0.0.1");
            return new DiscoveryNode(
                "node-" + index,
                new TransportAddress(new InetSocketAddress(address, 9300 + index)),
                Collections.emptyMap(),
                new HashSet<>(Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)),
                Version.CURRENT
            );
        } catch (Exception e) {
            throw new RuntimeException("Error creating dummy DiscoveryNode", e);
        }
    }
}
