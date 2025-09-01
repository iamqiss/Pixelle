/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.awarenesshealth;

import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.XContentParser;
import org.density.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ClusterAwarenessAttributesHealthSerializationTests extends AbstractSerializingTestCase<ClusterAwarenessAttributesHealth> {

    @Override
    protected ClusterAwarenessAttributesHealth doParseInstance(XContentParser parser) throws IOException {
        return ClusterAwarenessAttributesHealth.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ClusterAwarenessAttributesHealth> instanceReader() {
        return ClusterAwarenessAttributesHealth::new;
    }

    @Override
    protected ClusterAwarenessAttributesHealth createTestInstance() {
        Map<String, ClusterAwarenessAttributeValueHealth> clusterAwarenessAttributeValueHealthMap = new HashMap<>();
        ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth1 = new ClusterAwarenessAttributeValueHealth(
            "zone-1",
            2,
            0,
            0,
            2,
            2,
            1.0
        );

        ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth2 = new ClusterAwarenessAttributeValueHealth(
            "zone-2",
            2,
            0,
            0,
            2,
            2,
            1.0
        );

        ClusterAwarenessAttributeValueHealth clusterAwarenessAttributeValueHealth3 = new ClusterAwarenessAttributeValueHealth(
            "zone-3",
            2,
            0,
            0,
            2,
            2,
            0.0
        );

        clusterAwarenessAttributeValueHealthMap.put(clusterAwarenessAttributeValueHealth1.getName(), clusterAwarenessAttributeValueHealth1);
        clusterAwarenessAttributeValueHealthMap.put(clusterAwarenessAttributeValueHealth2.getName(), clusterAwarenessAttributeValueHealth2);
        clusterAwarenessAttributeValueHealthMap.put(clusterAwarenessAttributeValueHealth3.getName(), clusterAwarenessAttributeValueHealth3);

        return new ClusterAwarenessAttributesHealth("zone", clusterAwarenessAttributeValueHealthMap);
    }
}
