/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.rest;

import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.common.annotation.ExperimentalApi;
import org.density.core.rest.RestStatus;
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.rest.action.RestResponseListener;
import org.density.rule.action.DeleteRuleAction;
import org.density.rule.action.DeleteRuleRequest;
import org.density.rule.autotagging.FeatureType;
import org.density.transport.client.node.NodeClient;

import java.util.List;

import static org.density.rest.RestRequest.Method.DELETE;
import static org.density.rule.autotagging.Rule.ID_STRING;
import static org.density.rule.rest.RestGetRuleAction.FEATURE_TYPE;

/**
 * Rest action to delete a Rule
 * @density.experimental
 */
@ExperimentalApi
public class RestDeleteRuleAction extends BaseRestHandler {
    /**
     * Constructor for RestDeleteRuleAction
     */
    public RestDeleteRuleAction() {}

    @Override
    public String getName() {
        return "delete_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(DELETE, "_rules/{featureType}/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String ruleId = request.param(ID_STRING);
        FeatureType featureType = FeatureType.from(request.param(FEATURE_TYPE));
        DeleteRuleRequest deleteRuleRequest = new DeleteRuleRequest(ruleId, featureType);
        return channel -> client.execute(DeleteRuleAction.INSTANCE, deleteRuleRequest, deleteRuleResponse(channel));
    }

    private RestResponseListener<AcknowledgedResponse> deleteRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final AcknowledgedResponse response) throws Exception {
                return new BytesRestResponse(
                    RestStatus.OK,
                    channel.newBuilder().startObject().field("acknowledged", response.isAcknowledged()).endObject()
                );
            }
        };
    }
}
