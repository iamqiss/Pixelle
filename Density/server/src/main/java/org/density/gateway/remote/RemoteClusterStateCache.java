/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.gateway.remote;

import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.coordination.ClusterStateTermVersion;
import org.density.common.collect.Tuple;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Cache to Remote Cluster State based on term-version check. The current implementation
 * caches the last highest version of cluster-state that was downloaded from cache.
 *
 * @density.internal
 */
public class RemoteClusterStateCache {

    private final AtomicReference<Tuple<ClusterStateTermVersion, ClusterState>> clusterStateFromCache = new AtomicReference<>();

    public ClusterState getState(String clusterName, ClusterMetadataManifest manifest) {
        Tuple<ClusterStateTermVersion, ClusterState> cache = clusterStateFromCache.get();
        if (cache != null) {
            ClusterStateTermVersion manifestStateTermVersion = new ClusterStateTermVersion(
                new ClusterName(clusterName),
                manifest.getClusterUUID(),
                manifest.getClusterTerm(),
                manifest.getStateVersion()
            );
            if (cache.v1().equals(manifestStateTermVersion)) {
                return cache.v2();
            }
        }
        return null;
    }

    public void putState(final ClusterState newState) {
        if (newState.metadata() == null || newState.coordinationMetadata() == null) {
            // ensure the remote cluster state has coordination metadata set
            return;
        }

        ClusterStateTermVersion cacheStateTermVersion = new ClusterStateTermVersion(
            new ClusterName(newState.getClusterName().value()),
            newState.metadata().clusterUUID(),
            newState.term(),
            newState.version()
        );
        clusterStateFromCache.set(new Tuple<>(cacheStateTermVersion, newState));
    }
}
