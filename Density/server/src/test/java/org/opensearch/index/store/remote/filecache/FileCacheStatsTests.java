/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store.remote.filecache;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

import java.io.IOException;

public class FileCacheStatsTests extends DensityTestCase {

    private static final long BYTES_IN_GB = 1024 * 1024 * 1024;

    public static FileCacheStats getMockFullFileCacheStats() {
        final long active = randomLongBetween(100000, BYTES_IN_GB);
        final long total = randomLongBetween(100000, BYTES_IN_GB);
        final long used = randomLongBetween(100000, BYTES_IN_GB);
        final long pinned = randomLongBetween(100000, BYTES_IN_GB);
        final long evicted = randomLongBetween(0, active);
        final long hits = randomLongBetween(0, 10);
        final long misses = randomLongBetween(0, 10);

        return new FileCacheStats(
            active,
            total,
            used,
            pinned,
            evicted,
            hits,
            misses,
            AggregateFileCacheStats.FileCacheStatsType.OVER_ALL_STATS
        );
    }

    public static void validateFullFileCacheStats(FileCacheStats expected, FileCacheStats actual) {
        assertEquals(expected.getActive(), actual.getActive());
        assertEquals(expected.getUsed(), actual.getUsed());
        assertEquals(expected.getEvicted(), actual.getEvicted());
        assertEquals(expected.getHits(), actual.getHits());
        assertEquals(expected.getActivePercent(), actual.getActivePercent());
    }

    public void testFullFileCacheStatsSerialization() throws IOException {
        final FileCacheStats fileCacheStats = getMockFullFileCacheStats();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            fileCacheStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                validateFullFileCacheStats(fileCacheStats, new FileCacheStats(in));
            }
        }

    }
}
