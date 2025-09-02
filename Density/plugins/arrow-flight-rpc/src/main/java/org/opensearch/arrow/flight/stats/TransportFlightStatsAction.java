/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.stats;

import org.density.action.FailedNodeException;
import org.density.action.support.ActionFilters;
import org.density.action.support.nodes.TransportNodesAction;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for collecting Flight statistics from nodes
 */
public class TransportFlightStatsAction extends TransportNodesAction<
    FlightStatsRequest,
    FlightStatsResponse,
    FlightStatsRequest.NodeRequest,
    FlightNodeStats> {

    private final FlightStatsCollector statsCollector;

    /**
     * Creates a new transport action for Flight statistics collection
     * @param threadPool the thread pool
     * @param clusterService the cluster service
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param statsCollector the stats collector
     */
    @Inject
    public TransportFlightStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        FlightStatsCollector statsCollector
    ) {
        super(
            FlightStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            FlightStatsRequest::new,
            FlightStatsRequest.NodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            FlightNodeStats.class
        );
        this.statsCollector = statsCollector;
    }

    /** {@inheritDoc}
     * @param request the request
     * @param responses the responses
     * @param failures the failures */
    @Override
    protected FlightStatsResponse newResponse(
        FlightStatsRequest request,
        List<FlightNodeStats> responses,
        List<FailedNodeException> failures
    ) {
        return new FlightStatsResponse(clusterService.getClusterName(), responses, failures);
    }

    /** {@inheritDoc}
     * @param request the request */
    @Override
    protected FlightStatsRequest.NodeRequest newNodeRequest(FlightStatsRequest request) {
        return new FlightStatsRequest.NodeRequest();
    }

    /** {@inheritDoc}
     * @param in the stream input */
    @Override
    protected FlightNodeStats newNodeResponse(StreamInput in) throws IOException {
        return new FlightNodeStats(in);
    }

    /** {@inheritDoc}
     * @param request the node request */
    @Override
    protected FlightNodeStats nodeOperation(FlightStatsRequest.NodeRequest request) {
        FlightMetrics metrics = statsCollector.collectStats();
        return new FlightNodeStats(clusterService.localNode(), metrics);
    }
}
