/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.stats;

import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.GET;

/**
 * REST handler for Flight transport statistics
 */
public class FlightStatsRestHandler extends BaseRestHandler {

    /** Creates a new Flight stats REST handler */
    public FlightStatsRestHandler() {}

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return "flight_stats";
    }

    /** {@inheritDoc} */
    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_flight/stats"),
            new Route(GET, "/_flight/stats/{nodeId}"),
            new Route(GET, "/_nodes/flight/stats"),
            new Route(GET, "/_nodes/{nodeId}/flight/stats")
        );
    }

    /** {@inheritDoc}
     * @param request the REST request
     * @param client the node client */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String[] nodeIds = request.paramAsStringArray("nodeId", null);

        FlightStatsRequest flightStatsRequest = new FlightStatsRequest(nodeIds);
        flightStatsRequest.timeout(request.param("timeout"));

        return channel -> client.execute(
            FlightStatsAction.INSTANCE,
            flightStatsRequest,
            new RestToXContentListener<FlightStatsResponse>(channel)
        );
    }
}
