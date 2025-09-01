/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.routing;

import org.density.common.settings.Settings;
import org.density.test.DensityIntegTestCase;
import org.junit.After;

import static org.density.cluster.routing.OperationRouting.IGNORE_AWARENESS_ATTRIBUTES;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class OperationRoutingAwarenessTests extends DensityIntegTestCase {

    @After
    public void cleanup() {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder().putNull("*")));
    }

    public void testToggleSearchAllocationAwareness() {
        OperationRouting routing = internalCluster().clusterService().operationRouting();

        // Update awareness settings
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone"))
            .get();
        assertThat(routing.getAwarenessAttributes().size(), equalTo(1));
        assertThat(routing.getAwarenessAttributes().get(0), equalTo("zone"));
        assertTrue(internalCluster().clusterService().operationRouting().ignoreAwarenessAttributes());

        // Unset ignore awareness attributes
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(IGNORE_AWARENESS_ATTRIBUTES, false))
            .get();
        // assert that awareness attributes hasn't changed
        assertThat(routing.getAwarenessAttributes().size(), equalTo(1));
        assertThat(routing.getAwarenessAttributes().get(0), equalTo("zone"));
        assertFalse(internalCluster().clusterService().operationRouting().isIgnoreAwarenessAttr());
        assertFalse(internalCluster().clusterService().operationRouting().ignoreAwarenessAttributes());

        // Set ignore awareness attributes to true
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(IGNORE_AWARENESS_ATTRIBUTES, true))
            .get();
        // assert that awareness attributes hasn't changed
        assertThat(routing.getAwarenessAttributes().size(), equalTo(1));
        assertThat(routing.getAwarenessAttributes().get(0), equalTo("zone"));
        assertTrue(routing.isIgnoreAwarenessAttr());
        assertTrue(internalCluster().clusterService().operationRouting().ignoreAwarenessAttributes());
    }
}
