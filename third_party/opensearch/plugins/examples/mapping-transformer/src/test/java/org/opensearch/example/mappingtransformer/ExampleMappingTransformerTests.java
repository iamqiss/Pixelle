/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.mappingtransformer;

import org.density.action.support.PlainActionFuture;
import org.density.test.DensityTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.density.example.mappingtransformer.ExampleMappingTransformer.AUTO_ADDED_FIELD_NAME;
import static org.density.example.mappingtransformer.ExampleMappingTransformer.DOC;
import static org.density.example.mappingtransformer.ExampleMappingTransformer.PROPERTIES;
import static org.density.example.mappingtransformer.ExampleMappingTransformer.TRIGGER_FIELD_NAME;

public class ExampleMappingTransformerTests extends DensityTestCase {
    private final ExampleMappingTransformer transformer = new ExampleMappingTransformer();

    public void testExampleMappingTransformer_whenMappingWithoutDocLayer() {
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        mapping.put(PROPERTIES, properties);
        properties.put("dummyField", Map.of("type", "text"));

        transformer.transform(mapping, null, new PlainActionFuture<>());

        // verify no field is auto-injected
        assertFalse(
            "No trigger field in the mapping so should not transform the mapping to inject the field.",
            properties.containsKey(AUTO_ADDED_FIELD_NAME)
        );

        properties.put(TRIGGER_FIELD_NAME, Map.of("type", "text"));
        transformer.transform(mapping, null, new PlainActionFuture<>());

        // verify the field should be auto added to the mapping
        assertTrue(
            "The mapping has the trigger field so should transform the mapping to auto inject a field.",
            properties.containsKey(AUTO_ADDED_FIELD_NAME)
        );
    }

    public void testExampleMappingTransformer_whenMappingWithDocLayer() {
        Map<String, Object> doc = new HashMap<>();
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        doc.put(DOC, mapping);
        mapping.put(PROPERTIES, properties);
        properties.put("dummyField", Map.of("type", "text"));

        transformer.transform(doc, null, new PlainActionFuture<>());

        // verify no field is auto-injected
        assertFalse(
            "No trigger field in the mapping so should not transform the mapping to inject the field.",
            properties.containsKey(AUTO_ADDED_FIELD_NAME)
        );

        properties.put(TRIGGER_FIELD_NAME, Map.of("type", "text"));
        transformer.transform(mapping, null, new PlainActionFuture<>());

        // verify the field should be auto added to the mapping
        assertTrue(
            "The mapping has the trigger field so should transform the mapping to auto inject a field.",
            properties.containsKey(AUTO_ADDED_FIELD_NAME)
        );
    }

    public void testExampleMappingTransformer_whenNoMapping_thenDoNothing() {
        transformer.transform(null, null, new PlainActionFuture<>());
    }

    public void testExampleMappingTransformer_whenEmptyMapping_thenDoNothing() {
        Map<String, Object> doc = new HashMap<>();
        transformer.transform(doc, null, new PlainActionFuture<>());
        assertEquals(new HashMap<>(), doc);
    }
}
