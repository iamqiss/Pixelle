/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.startree.filter;

import org.density.common.annotation.ExperimentalApi;
import org.density.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.density.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.density.search.internal.SearchContext;
import org.density.search.startree.StarTreeNodeCollector;

/**
 * Filter which matches no StarTreeNodes.
 */
@ExperimentalApi
public class MatchNoneFilter implements DimensionFilter {

    @Override
    public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) {
        // Nothing to do as we won't match anything.
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector) {
        // Don't match any star tree node.
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        return false;
    }

    @Override
    public String getDimensionName() {
        return null;
    }
}
