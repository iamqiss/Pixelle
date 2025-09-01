/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.rest;

import org.density.common.annotation.ExperimentalApi;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentParser;
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.rest.action.RestResponseListener;
import org.density.rule.action.UpdateRuleAction;
import org.density.rule.action.UpdateRuleRequest;
import org.density.rule.action.UpdateRuleResponse;
import org.density.rule.autotagging.FeatureType;
import org.density.rule.autotagging.Rule.Builder;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.PUT;
import static org.density.rule.autotagging.Rule.ID_STRING;
import static org.density.rule.rest.RestGetRuleAction.FEATURE_TYPE;

/**
 * Rest action to update a Rule
 * @density.experimental
 */
@ExperimentalApi
public class RestUpdateRuleAction extends BaseRestHandler {
    /**
     * constructor for RestUpdateRuleAction
     */
    public RestUpdateRuleAction() {}

    @Override
    public String getName() {
        return "update_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(PUT, "_rules/{featureType}/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final FeatureType featureType = FeatureType.from(request.param(FEATURE_TYPE));
        try (XContentParser parser = request.contentParser()) {
            Builder builder = Builder.fromXContent(parser, featureType);
            UpdateRuleRequest updateRuleRequest = new UpdateRuleRequest(
                request.param(ID_STRING),
                builder.getDescription(),
                builder.getAttributeMap(),
                builder.getFeatureValue(),
                featureType
            );
            return channel -> client.execute(UpdateRuleAction.INSTANCE, updateRuleRequest, updateRuleResponse(channel));
        }
    }

    private RestResponseListener<UpdateRuleResponse> updateRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final UpdateRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
