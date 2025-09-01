/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.search;

import org.density.action.search.DeletePitRequest;
import org.density.action.search.DeletePitResponse;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestStatusToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.density.rest.RestRequest.Method.DELETE;

/**
 * Rest action for deleting PIT contexts
 */
public class RestDeletePitAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "delete_pit_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String allPitIdsQualifier = "_all";
        final DeletePitRequest deletePITRequest;
        if (request.path().contains(allPitIdsQualifier)) {
            deletePITRequest = new DeletePitRequest(asList(allPitIdsQualifier));
        } else {
            deletePITRequest = new DeletePitRequest();
            request.withContentOrSourceParamParserOrNull((xContentParser -> {
                if (xContentParser != null) {
                    try {
                        deletePITRequest.fromXContent(xContentParser);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to parse request body", e);
                    }
                }
            }));
        }
        return channel -> client.deletePits(deletePITRequest, new RestStatusToXContentListener<DeletePitResponse>(channel));
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(DELETE, "/_search/point_in_time"), new Route(DELETE, "/_search/point_in_time/_all")));
    }
}
