/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.startree;

import org.density.common.annotation.ExperimentalApi;
import org.density.index.compositeindex.datacube.startree.node.StarTreeNode;

/**
 * Collects one or more @{@link StarTreeNode}'s
 */
@ExperimentalApi
public interface StarTreeNodeCollector {
    /**
     * Called to collect a @{@link StarTreeNode}
     * @param node : Node to collect
     */
    void collectStarTreeNode(StarTreeNode node);

}
