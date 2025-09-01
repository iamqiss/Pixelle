/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.upgrades;

import org.density.client.Request;
import org.density.client.Response;
import org.density.test.rest.yaml.ObjectPath;

import java.util.Map;

public class PluginInfoIT extends AbstractFullClusterRestartTestCase {
    public void testPluginInfoSerialization() throws Exception {
        // Ensure all nodes are able to come up, validate with GET _nodes.
        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        // Any issue in PluginInfo serialization logic will result into connection failures
        // and hence reduced number of nodes.
        assertEquals(2, nodeMap.keySet().size());
    }
}
