/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.metadata;

import org.density.cluster.ClusterModule;
import org.density.cluster.Diff;
import org.density.cluster.routing.WeightedRouting;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.XContentParser;
import org.density.test.AbstractDiffableSerializationTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WeightedRoutingMetadataTests extends AbstractDiffableSerializationTestCase<Metadata.Custom> {

    @Override
    protected Writeable.Reader<Metadata.Custom> instanceReader() {
        return WeightedRoutingMetadata::new;
    }

    @Override
    protected WeightedRoutingMetadata createTestInstance() {
        String attributeName = "zone";
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        if (randomBoolean()) {
            weights = new HashMap<>();
            attributeName = "";
        }
        WeightedRouting weightedRouting = new WeightedRouting(attributeName, weights);
        WeightedRoutingMetadata weightedRoutingMetadata = new WeightedRoutingMetadata(weightedRouting, -1);

        return weightedRoutingMetadata;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    @Override
    protected WeightedRoutingMetadata doParseInstance(XContentParser parser) throws IOException {
        return WeightedRoutingMetadata.fromXContent(parser);
    }

    @Override
    protected Metadata.Custom makeTestChanges(Metadata.Custom testInstance) {

        WeightedRouting weightedRouting = new WeightedRouting("", new HashMap<>());
        WeightedRoutingMetadata weightedRoutingMetadata = new WeightedRoutingMetadata(weightedRouting, -1);
        return weightedRoutingMetadata;
    }

    @Override
    protected Writeable.Reader<Diff<Metadata.Custom>> diffReader() {
        return WeightedRoutingMetadata::readDiffFrom;
    }

}
