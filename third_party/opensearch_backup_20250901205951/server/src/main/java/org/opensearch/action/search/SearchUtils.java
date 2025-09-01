/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.action.StepListener;
import org.density.cluster.ClusterState;
import org.density.cluster.node.DiscoveryNode;
import org.density.core.action.ActionListener;
import org.density.transport.RemoteClusterService;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * Helper class for common search functions
 */
public class SearchUtils {

    public SearchUtils() {}

    /**
     * Get connection lookup listener for list of clusters passed
     */
    public static ActionListener<BiFunction<String, String, DiscoveryNode>> getConnectionLookupListener(
        RemoteClusterService remoteClusterService,
        ClusterState state,
        Set<String> clusters
    ) {
        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = new StepListener<>();

        if (clusters.isEmpty()) {
            lookupListener.onResponse((cluster, nodeId) -> state.getNodes().get(nodeId));
        } else {
            remoteClusterService.collectNodes(clusters, lookupListener);
        }
        return lookupListener;
    }
}
