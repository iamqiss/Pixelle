/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

import org.density.telemetry.OTelAttributesConverter;
import org.density.telemetry.metrics.tags.Tags;
import org.density.telemetry.tracing.attributes.Attributes;
import org.density.test.DensityTestCase;

import java.util.Map;

import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.internal.InternalAttributeKeyImpl;

public class OTelAttributesConverterTests extends DensityTestCase {

    public void testConverterNullAttributes() {
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert((Attributes) null);
        assertEquals(0, otelAttributes.size());
    }

    public void testConverterEmptyAttributes() {
        Attributes attributes = Attributes.EMPTY;
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert(attributes);
        assertEquals(0, otelAttributes.size());
    }

    public void testConverterSingleAttributes() {
        Attributes attributes = Attributes.create().addAttribute("key1", "value");
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert(attributes);
        assertEquals(1, otelAttributes.size());
        assertEquals("value", otelAttributes.get(InternalAttributeKeyImpl.create("key1", AttributeType.STRING)));
    }

    public void testConverterMultipleAttributes() {
        Attributes attributes = Attributes.create()
            .addAttribute("key1", 1l)
            .addAttribute("key2", 1.0)
            .addAttribute("key3", true)
            .addAttribute("key4", "value4");
        Map<String, ?> attributeMap = attributes.getAttributesMap();
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert(attributes);
        assertEquals(4, otelAttributes.size());
        otelAttributes.asMap().forEach((x, y) -> assertEquals(attributeMap.get(x.getKey()), y));
    }

    public void testConverterMultipleTags() {
        Tags tags = Tags.create().addTag("key1", 1l).addTag("key2", 1.0).addTag("key3", true).addTag("key4", "value4");
        Map<String, ?> tagsMap = tags.getTagsMap();
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert(tags);
        assertEquals(4, otelAttributes.size());
        otelAttributes.asMap().forEach((x, y) -> assertEquals(tagsMap.get(x.getKey()), y));
    }
}
