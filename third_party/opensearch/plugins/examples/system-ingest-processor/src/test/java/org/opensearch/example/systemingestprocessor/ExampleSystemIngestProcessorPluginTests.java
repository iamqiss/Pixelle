/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.example.systemingestprocessor;

import org.density.ingest.Processor;
import org.density.test.DensityTestCase;

import java.util.List;
import java.util.Map;

import static org.density.example.systemingestprocessor.ExampleSystemIngestProcessorPlugin.TRIGGER_SETTING;
import static org.mockito.Mockito.mock;

public class ExampleSystemIngestProcessorPluginTests extends DensityTestCase {
    private final ExampleSystemIngestProcessorPlugin plugin = new ExampleSystemIngestProcessorPlugin();
    private final Processor.Parameters parameters = mock(Processor.Parameters.class);

    public void testGetSystemIngestProcessors() {
        final Map<String, Processor.Factory> factories = plugin.getSystemIngestProcessors(parameters);

        assertTrue(
            "Should return the example system ingest processor factory.",
            factories.get(ExampleSystemIngestProcessorFactory.TYPE) instanceof ExampleSystemIngestProcessorFactory
        );
    }

    public void testGetSettings() {
        assertEquals(List.of(TRIGGER_SETTING), plugin.getSettings());
    }
}
