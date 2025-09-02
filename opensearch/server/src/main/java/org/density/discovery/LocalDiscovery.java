/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.discovery;

import org.density.cluster.ClusterChangedEvent;
import org.density.cluster.ClusterState;
import org.density.cluster.NodeConnectionsService;
import org.density.cluster.coordination.PendingClusterStateStats;
import org.density.cluster.coordination.PublishClusterStateStats;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterApplier;
import org.density.cluster.service.ClusterStateStats;
import org.density.common.lifecycle.AbstractLifecycleComponent;
import org.density.core.action.ActionListener;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Clusterless implementation of Discovery. This is only able to "discover" the local node.
 */
public class LocalDiscovery extends AbstractLifecycleComponent implements Discovery {
    private static final DiscoveryStats EMPTY_STATS = new DiscoveryStats(
        new PendingClusterStateStats(0, 0, 0),
        new PublishClusterStateStats(0, 0, 0),
        new ClusterStateStats()
    );
    private final TransportService transportService;
    private final ClusterApplier clusterApplier;

    public LocalDiscovery(TransportService transportService, ClusterApplier clusterApplier) {
        this.transportService = transportService;
        this.clusterApplier = clusterApplier;
    }

    @Override
    public void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener) {
        // In clusterless mode, we should never be asked to publish a cluster state.
        throw new UnsupportedOperationException("Should not be called in clusterless mode");
    }

    @Override
    protected void doStart() {
        DiscoveryNode localNode = transportService.getLocalNode();
        ClusterState bootstrapClusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build())
            .build();
        clusterApplier.setInitialState(bootstrapClusterState);
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public DiscoveryStats stats() {
        return EMPTY_STATS;
    }

    @Override
    public void startInitialJoin() {

    }

    @Override
    public void setNodeConnectionsService(NodeConnectionsService nodeConnectionsService) {

    }
}
