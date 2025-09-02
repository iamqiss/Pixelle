/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.rest;

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
import org.density.rule.action.CreateRuleAction;
import org.density.rule.action.CreateRuleRequest;
import org.density.rule.action.CreateRuleResponse;
import org.density.rule.autotagging.FeatureType;
import org.density.rule.autotagging.Rule.Builder;
import org.density.transport.client.node.NodeClient;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;

import static org.density.rest.RestRequest.Method.PUT;
import static org.density.rule.rest.RestGetRuleAction.FEATURE_TYPE;

/**
 * Rest action to create a Rule
 * @density.experimental
 */
public class RestCreateRuleAction extends BaseRestHandler {
    /**
     * constructor for RestCreateRuleAction
     */
    public RestCreateRuleAction() {}

    @Override
    public String getName() {
        return "create_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(PUT, "_rules/{featureType}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final FeatureType featureType = FeatureType.from(request.param(FEATURE_TYPE));
        try (XContentParser parser = request.contentParser()) {
            Builder builder = Builder.fromXContent(parser, featureType);
            CreateRuleRequest createRuleRequest = new CreateRuleRequest(builder.updatedAt(Instant.now().toString()).id().build());
            return channel -> client.execute(CreateRuleAction.INSTANCE, createRuleRequest, createRuleResponse(channel));
        }
    }

    private RestResponseListener<CreateRuleResponse> createRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final CreateRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
