/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.indices;

import org.density.action.admin.indices.streamingingestion.pause.PauseIngestionRequest;
import org.density.action.support.IndicesOptions;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.logging.DeprecationLogger;
import org.density.core.common.Strings;
import org.density.rest.BaseRestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.density.rest.RestRequest.Method.POST;

/**
 * Transport action to pause pull-based ingestion.
 *
 * @density.experimental
 */
@ExperimentalApi
public class RestPauseIngestionAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestPauseIngestionAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/{index}/ingestion/_pause")));
    }

    @Override
    public String getName() {
        return "pause_ingestion_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        PauseIngestionRequest pauseIngestionRequest = new PauseIngestionRequest(Strings.splitStringByCommaToArray(request.param("index")));
        pauseIngestionRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", pauseIngestionRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(pauseIngestionRequest, request, deprecationLogger, getName());
        pauseIngestionRequest.timeout(request.paramAsTime("timeout", pauseIngestionRequest.timeout()));
        pauseIngestionRequest.indicesOptions(IndicesOptions.fromRequest(request, pauseIngestionRequest.indicesOptions()));

        return channel -> client.admin().indices().pauseIngestion(pauseIngestionRequest, new RestToXContentListener<>(channel));
    }

}
