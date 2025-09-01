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

public class ClusterAwarenessAttributeValueHealthTests extends AbstractSerializingTestCase<ClusterAwarenessAttributeValueHealth> {

    @Override
    protected Writeable.Reader<ClusterAwarenessAttributeValueHealth> instanceReader() {
        return ClusterAwarenessAttributeValueHealth::new;
    }

    @Override
    protected ClusterAwarenessAttributeValueHealth createTestInstance() {
        return new ClusterAwarenessAttributeValueHealth(
            randomFrom("zone-1", "zone-2", "zone-3"),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomFrom(0.0, 1.0)
        );
    }

    @Override
    protected ClusterAwarenessAttributeValueHealth doParseInstance(XContentParser parser) throws IOException {
        return ClusterAwarenessAttributeValueHealth.fromXContent(parser);
    }
}
