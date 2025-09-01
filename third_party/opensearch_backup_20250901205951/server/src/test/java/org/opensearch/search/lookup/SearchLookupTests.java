/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.lookup;

import org.density.index.mapper.MapperService;
import org.density.test.DensityTestCase;

import static org.mockito.Mockito.mock;

public class SearchLookupTests extends DensityTestCase {
    public void testDeprecatedConstructorShardId() {
        final SearchLookup searchLookup = new SearchLookup(mock(MapperService.class), (a, b) -> null);
        assertThrows(IllegalStateException.class, searchLookup::shardId);
    }
}
