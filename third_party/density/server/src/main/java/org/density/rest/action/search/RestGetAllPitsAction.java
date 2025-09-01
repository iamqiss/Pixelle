
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.search;

import org.density.action.search.GetAllPitNodesRequest;
import org.density.action.search.GetAllPitNodesResponse;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.XContentBuilder;
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.rest.action.RestBuilderListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.unmodifiableList;
import static org.density.rest.RestRequest.Method.GET;

/**
 * Rest action for retrieving all active PIT IDs across all nodes
 */
public class RestGetAllPitsAction extends BaseRestHandler {

    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestGetAllPitsAction(Supplier<DiscoveryNodes> nodesInCluster) {
        super();
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public String getName() {
        return "get_all_pit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        for (DiscoveryNode node : nodesInCluster.get()) {
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = nodes.toArray(new DiscoveryNode[0]);
        GetAllPitNodesRequest getAllPitNodesRequest = new GetAllPitNodesRequest(disNodesArr);
        return channel -> client.getAllPits(getAllPitNodesRequest, new RestBuilderListener<GetAllPitNodesResponse>(channel) {
            @Override
            public RestResponse buildResponse(final GetAllPitNodesResponse getAllPITNodesResponse, XContentBuilder builder)
                throws Exception {
                builder.startObject();
                if (getAllPITNodesResponse.hasFailures()) {
                    builder.startArray("failures");
                    for (int idx = 0; idx < getAllPITNodesResponse.failures().size(); idx++) {
                        builder.startObject();
                        builder.field(
                            getAllPITNodesResponse.failures().get(idx).nodeId(),
                            getAllPITNodesResponse.failures().get(idx).getDetailedMessage()
                        );
                        builder.endObject();
                    }
                    builder.endArray();
                }
                builder.field("pits", getAllPITNodesResponse.getPitInfos());
                builder.endObject();
                if (getAllPITNodesResponse.hasFailures() && getAllPITNodesResponse.getPitInfos().isEmpty()) {
                    return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
                }
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(Collections.singletonList(new Route(GET, "/_search/point_in_time/_all")));
    }
}
