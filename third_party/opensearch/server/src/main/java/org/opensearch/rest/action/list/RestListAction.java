/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.list;

import org.density.core.rest.RestStatus;
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestRequest;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.density.rest.RestRequest.Method.GET;

/**
 * Base _list API endpoint
 *
 * @density.api
 */
public class RestListAction extends BaseRestHandler {

    private static final String LIST = ":‑|";
    private static final String LIST_NL = LIST + "\n";
    private final String HELP;

    public RestListAction(List<AbstractListAction> listActions) {
        StringBuilder sb = new StringBuilder();
        sb.append(LIST_NL);
        for (AbstractListAction listAction : listActions) {
            listAction.documentation(sb);
        }
        HELP = sb.toString();
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_list"));
    }

    @Override
    public String getName() {
        return "list_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, HELP));
    }

}
