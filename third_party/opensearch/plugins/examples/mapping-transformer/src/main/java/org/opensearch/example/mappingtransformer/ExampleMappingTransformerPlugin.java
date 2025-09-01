/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.mappingtransformer;

import org.density.index.mapper.MappingTransformer;
import org.density.plugins.MapperPlugin;
import org.density.plugins.Plugin;

import java.util.List;

/**
 * Example plugin that implements a custom mapping transformer.
 */
public class ExampleMappingTransformerPlugin extends Plugin implements MapperPlugin {

    /**
     * Constructs a new ExampleMappingTransformerPlugin
     */
    public ExampleMappingTransformerPlugin() {}

    /**
     * @return Available mapping transformer
     */
    @Override
    public List<MappingTransformer> getMappingTransformers() {
        return List.of(new ExampleMappingTransformer());
    }
}
