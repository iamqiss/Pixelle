/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.tasks;

import org.density.test.DensityTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.density.tasks.TaskResourceTrackingService.TASK_ID;

public class TaskThreadContextStatePropagatorTests extends DensityTestCase {
    private final TaskThreadContextStatePropagator taskThreadContextStatePropagator = new TaskThreadContextStatePropagator();

    public void testTransient() {
        Map<String, Object> transientHeader = new HashMap<>();
        transientHeader.put(TASK_ID, "t_1");
        Map<String, Object> transientPropagatedHeader = taskThreadContextStatePropagator.transients(transientHeader, false);
        assertEquals("t_1", transientPropagatedHeader.get(TASK_ID));
    }

    public void testTransientForSystemContext() {
        Map<String, Object> transientHeader = new HashMap<>();
        transientHeader.put(TASK_ID, "t_1");
        Map<String, Object> transientPropagatedHeader = taskThreadContextStatePropagator.transients(transientHeader, true);
        assertEquals("t_1", transientPropagatedHeader.get(TASK_ID));
    }
}
