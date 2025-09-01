/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.mappingtransformer;

import org.density.index.mapper.MappingTransformer;
import org.density.test.DensityTestCase;

import java.util.List;

public class ExampleMappingTransformerPluginTests extends DensityTestCase {
    private final ExampleMappingTransformerPlugin plugin = new ExampleMappingTransformerPlugin();

    public void testGetMappingTransformers() {
        List<MappingTransformer> mappingTransformerList = plugin.getMappingTransformers();

        assertTrue("Should return an example mapping transformer.", mappingTransformerList.getFirst() instanceof ExampleMappingTransformer);
    }
}
