/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.action.ingest;

import org.density.action.ActionListenerResponseHandler;
import org.density.action.ActionRequest;
import org.density.action.ActionType;
import org.density.cluster.ClusterChangedEvent;
import org.density.cluster.ClusterStateApplier;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.Randomness;
import org.density.core.action.ActionListener;
import org.density.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A utility for forwarding ingest requests to ingest nodes in a round-robin fashion.
 * <p>
 * TODO: move this into IngestService and make index/bulk actions call that
 *
 * @density.internal
 */
public final class IngestActionForwarder implements ClusterStateApplier {

    private final TransportService transportService;
    private final AtomicInteger ingestNodeGenerator = new AtomicInteger(Randomness.get().nextInt());
    private DiscoveryNode[] ingestNodes;

    public IngestActionForwarder(TransportService transportService) {
        this.transportService = transportService;
        ingestNodes = new DiscoveryNode[0];
    }

    public void forwardIngestRequest(ActionType<?> action, ActionRequest request, ActionListener<?> listener) {
        transportService.sendRequest(
            randomIngestNode(),
            action.name(),
            request,
            new ActionListenerResponseHandler(listener, action.getResponseReader())
        );
    }

    private DiscoveryNode randomIngestNode() {
        final DiscoveryNode[] nodes = ingestNodes;
        if (nodes.length == 0) {
            throw new IllegalStateException("There are no ingest nodes in this cluster, unable to forward request to an ingest node.");
        }

        return nodes[Math.floorMod(ingestNodeGenerator.incrementAndGet(), nodes.length)];
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        ingestNodes = event.state().getNodes().getIngestNodes().values().toArray(new DiscoveryNode[0]);
    }
}
