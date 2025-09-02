/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm;

import org.density.test.DensityTestCase;

public class ResourceTypeTests extends DensityTestCase {

    public void testFromName() {
        assertSame(ResourceType.CPU, ResourceType.fromName("cpu"));
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("CPU"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Cpu"); });

        assertSame(ResourceType.MEMORY, ResourceType.fromName("memory"));
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Memory"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("MEMORY"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("JVM"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Heap"); });
        assertThrows(IllegalArgumentException.class, () -> { ResourceType.fromName("Disk"); });
    }

    public void testGetName() {
        assertEquals("cpu", ResourceType.CPU.getName());
        assertEquals("memory", ResourceType.MEMORY.getName());
    }
}
