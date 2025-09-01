/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.util;

import org.density.test.DensityTestCase;

public class StreakTests extends DensityTestCase {

    public void testStreak() {
        Streak streak = new Streak();

        // Streak starts with zero.
        assertEquals(0, streak.length());

        // Streak increases on successive successful events.
        streak.record(true);
        assertEquals(1, streak.length());
        streak.record(true);
        assertEquals(2, streak.length());
        streak.record(true);
        assertEquals(3, streak.length());

        // Streak resets to zero after an unsuccessful event.
        streak.record(false);
        assertEquals(0, streak.length());
    }
}
