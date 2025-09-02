/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.backpressure.stats;

import org.density.core.common.io.stream.Writeable;
import org.density.search.backpressure.settings.SearchBackpressureMode;
import org.density.test.AbstractWireSerializingTestCase;

public class SearchBackpressureStatsTests extends AbstractWireSerializingTestCase<SearchBackpressureStats> {
    @Override
    protected Writeable.Reader<SearchBackpressureStats> instanceReader() {
        return SearchBackpressureStats::new;
    }

    @Override
    protected SearchBackpressureStats createTestInstance() {
        return randomInstance();
    }

    public static SearchBackpressureStats randomInstance() {
        return new SearchBackpressureStats(
            SearchTaskStatsTests.randomInstance(),
            SearchShardTaskStatsTests.randomInstance(),
            randomFrom(SearchBackpressureMode.DISABLED, SearchBackpressureMode.MONITOR_ONLY, SearchBackpressureMode.ENFORCED)
        );
    }
}
