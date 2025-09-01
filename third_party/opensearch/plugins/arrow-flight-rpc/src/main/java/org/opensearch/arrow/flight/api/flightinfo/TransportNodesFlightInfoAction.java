/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.api.flightinfo;

import org.density.action.FailedNodeException;
import org.density.action.support.ActionFilters;
import org.density.action.support.nodes.TransportNodesAction;
import org.density.arrow.flight.bootstrap.FlightService;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for getting flight information from nodes
 */
public class TransportNodesFlightInfoAction extends TransportNodesAction<
    NodesFlightInfoRequest,
    NodesFlightInfoResponse,
    NodesFlightInfoRequest.NodeFlightInfoRequest,
    NodeFlightInfo> {

    private final FlightService flightService;

    /**
     * Constructor for TransportNodesFlightInfoAction
     * @param settings The settings for the action
     * @param threadPool The thread pool for the action
     * @param clusterService The cluster service for the action
     * @param transportService The transport service for the action
     * @param actionFilters The action filters for the action
     * @param flightService The flight service for the action
     */
    @Inject
    public TransportNodesFlightInfoAction(
        Settings settings,
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        FlightService flightService
    ) {
        super(
            NodesFlightInfoAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesFlightInfoRequest::new,
            NodesFlightInfoRequest.NodeFlightInfoRequest::new,
            ThreadPool.Names.MANAGEMENT,
            NodeFlightInfo.class
        );
        this.flightService = flightService;
    }

    /**
     * Creates a new response object for the action.
     * @param request The associated request.
     * @param nodeFlightInfos All successful node-level responses.
     * @param failures All node-level failures.
     * @return The response object.
     */
    @Override
    protected NodesFlightInfoResponse newResponse(
        NodesFlightInfoRequest request,
        List<NodeFlightInfo> nodeFlightInfos,
        List<FailedNodeException> failures
    ) {
        return new NodesFlightInfoResponse(clusterService.getClusterName(), nodeFlightInfos, failures);
    }

    /**
     * Creates a new request object for a node.
     * @param request The associated request.
     * @return The request object.
     */
    @Override
    protected NodesFlightInfoRequest.NodeFlightInfoRequest newNodeRequest(NodesFlightInfoRequest request) {
        return new NodesFlightInfoRequest.NodeFlightInfoRequest(request);
    }

    /**
     * Creates a new response object for a node.
     * @param in The stream input to read from.
     * @return The response object.
     */
    @Override
    protected NodeFlightInfo newNodeResponse(StreamInput in) throws IOException {
        return new NodeFlightInfo(in);
    }

    /**
     * Creates a new response object for a node.
     * @param request The associated request.
     * @return The response object.
     */
    @Override
    protected NodeFlightInfo nodeOperation(NodesFlightInfoRequest.NodeFlightInfoRequest request) {
        return new NodeFlightInfo(clusterService.localNode(), flightService.getBoundAddress());
    }
}
