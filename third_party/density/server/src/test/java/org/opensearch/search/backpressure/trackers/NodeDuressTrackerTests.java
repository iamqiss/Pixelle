/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.backpressure.trackers;

import org.density.search.backpressure.trackers.NodeDuressTrackers.NodeDuressTracker;
import org.density.test.DensityTestCase;

import java.util.concurrent.atomic.AtomicReference;

public class NodeDuressTrackerTests extends DensityTestCase {

    public void testNodeDuressTracker() {
        AtomicReference<Double> cpuUsage = new AtomicReference<>(0.0);
        NodeDuressTracker tracker = new NodeDuressTracker(() -> cpuUsage.get() >= 0.5, () -> 3);

        // Node not in duress.
        assertFalse(tracker.test());

        // Node in duress; the streak must keep increasing.
        cpuUsage.set(0.7);
        assertFalse(tracker.test());
        assertFalse(tracker.test());
        assertTrue(tracker.test());

        // Node not in duress anymore.
        cpuUsage.set(0.3);
        assertFalse(tracker.test());
        assertFalse(tracker.test());
    }
}
