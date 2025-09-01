/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.systemingestprocessor;

import org.density.common.settings.Settings;
import org.density.ingest.AbstractBatchingSystemProcessor;
import org.density.test.DensityTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.density.example.systemingestprocessor.ExampleSystemIngestProcessorFactory.DOC;
import static org.density.example.systemingestprocessor.ExampleSystemIngestProcessorFactory.PROPERTIES;
import static org.density.example.systemingestprocessor.ExampleSystemIngestProcessorFactory.TRIGGER_FIELD_NAME;
import static org.density.example.systemingestprocessor.ExampleSystemIngestProcessorPlugin.TRIGGER_SETTING;
import static org.density.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_MAPPINGS;
import static org.density.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_SETTINGS;
import static org.density.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_TEMPLATE_MAPPINGS;
import static org.density.plugins.IngestPlugin.SystemIngestPipelineConfigKeys.INDEX_TEMPLATE_SETTINGS;

public class ExampleSystemIngestProcessorFactoryTests extends DensityTestCase {
    public void testNewProcessor_whenWithTriggerField_thenReturnProcessor() {
        final ExampleSystemIngestProcessorFactory factory = new ExampleSystemIngestProcessorFactory();
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        mapping.put(PROPERTIES, properties);
        properties.put(TRIGGER_FIELD_NAME, Map.of("type", "text"));

        AbstractBatchingSystemProcessor processor = factory.newProcessor("tag", "description", Map.of(INDEX_MAPPINGS, mapping));

        assertNotNull("Should create an example system ingest processor when the trigger field is found.", processor);
        assertTrue(processor instanceof ExampleSystemIngestProcessor);
    }

    public void testNewProcessor_whenWithoutTriggerField_thenReturnProcessor() {
        final ExampleSystemIngestProcessorFactory factory = new ExampleSystemIngestProcessorFactory();
        Map<String, Object> doc = new HashMap<>();
        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        doc.put(DOC, mapping);
        mapping.put(PROPERTIES, properties);

        AbstractBatchingSystemProcessor processor = factory.newProcessor("tag", null, Map.of(INDEX_TEMPLATE_MAPPINGS, List.of(doc)));

        assertNull("Should not create an example system ingest processor when the trigger field is not found.", processor);
    }

    public void testNewProcessor_whenNoMapping_thenReturnNull() {
        final ExampleSystemIngestProcessorFactory factory = new ExampleSystemIngestProcessorFactory();

        AbstractBatchingSystemProcessor processor = factory.newProcessor("tag", "description", Map.of());

        assertNull("Should not create an example system ingest processor when the mapping is not found.", processor);
    }

    public void testNewProcessor_whenWithTriggerSettingFromIndex_thenReturnProcessor() {
        final ExampleSystemIngestProcessorFactory factory = new ExampleSystemIngestProcessorFactory();
        Settings triggerEnabled = Settings.builder().put(TRIGGER_SETTING.getKey(), true).build();

        AbstractBatchingSystemProcessor processor = factory.newProcessor("tag", "description", Map.of(INDEX_SETTINGS, triggerEnabled));

        assertNotNull("Should create an example system ingest processor when the trigger_setting is true.", processor);
        assertTrue(processor instanceof ExampleSystemIngestProcessor);
    }

    public void testNewProcessor_whenWithTriggerSettingFromTemplate_thenReturnProcessor() {
        final ExampleSystemIngestProcessorFactory factory = new ExampleSystemIngestProcessorFactory();
        Settings triggerEnabled = Settings.builder().put(TRIGGER_SETTING.getKey(), true).build();

        AbstractBatchingSystemProcessor processor = factory.newProcessor(
            "tag",
            "description",
            Map.of(INDEX_TEMPLATE_SETTINGS, List.of(triggerEnabled))
        );

        assertNotNull("Should create an example system ingest processor when the trigger_setting is true.", processor);
        assertTrue(processor instanceof ExampleSystemIngestProcessor);
    }

    public void testNewProcessor_whenWithTriggerSettingDisabled_thenReturnProcessor() {
        final ExampleSystemIngestProcessorFactory factory = new ExampleSystemIngestProcessorFactory();
        Settings triggerDisabled = Settings.builder().put(TRIGGER_SETTING.getKey(), false).build();

        AbstractBatchingSystemProcessor processor = factory.newProcessor("tag", "description", Map.of(INDEX_SETTINGS, triggerDisabled));

        assertNull("Should not create an example system ingest processor when the trigger_setting is false.", processor);
    }
}
