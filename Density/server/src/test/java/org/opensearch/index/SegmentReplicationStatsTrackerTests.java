/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index;

import org.density.core.index.shard.ShardId;
import org.density.indices.IndicesService;
import org.density.test.DensityTestCase;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

public class SegmentReplicationStatsTrackerTests extends DensityTestCase {

    private IndicesService indicesService = mock(IndicesService.class);

    public void testRejectedCount() {
        SegmentReplicationStatsTracker segmentReplicationStatsTracker = new SegmentReplicationStatsTracker(indicesService);

        // Verify that total rejection count is 0 on an empty rejectionCount map in statsTracker.
        assertTrue(segmentReplicationStatsTracker.getRejectionCount().isEmpty());
        assertEquals(segmentReplicationStatsTracker.getTotalRejectionStats().getTotalRejectionCount(), 0L);

        // Verify that total rejection count is 1 after incrementing rejectionCount.
        segmentReplicationStatsTracker.incrementRejectionCount(Mockito.mock(ShardId.class));
        assertEquals(segmentReplicationStatsTracker.getTotalRejectionStats().getTotalRejectionCount(), 1L);
    }

}
