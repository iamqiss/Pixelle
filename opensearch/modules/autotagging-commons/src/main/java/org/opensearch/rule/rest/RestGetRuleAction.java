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
import org.density.rest.BaseRestHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.rest.action.RestResponseListener;
import org.density.rule.action.GetRuleAction;
import org.density.rule.action.GetRuleRequest;
import org.density.rule.action.GetRuleResponse;
import org.density.rule.autotagging.Attribute;
import org.density.rule.autotagging.FeatureType;
import org.density.transport.client.node.NodeClient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.density.rest.RestRequest.Method.GET;
import static org.density.rule.autotagging.Rule.ID_STRING;

/**
 * Rest action to get a Rule
 * @density.experimental
 */
@ExperimentalApi
public class RestGetRuleAction extends BaseRestHandler {
    /**
     * field name used for pagination
     */
    public static final String SEARCH_AFTER_STRING = "search_after";
    /**
     * field name to specify feature type
     */
    public static final String FEATURE_TYPE = "featureType";

    /**
     * constructor for RestGetRuleAction
     */
    public RestGetRuleAction() {}

    @Override
    public String getName() {
        return "get_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(GET, "_rules/{featureType}/"), new RestHandler.Route(GET, "_rules/{featureType}/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final Map<Attribute, Set<String>> attributeFilters = new HashMap<>();

        if (!request.hasParam(FEATURE_TYPE)) {
            throw new IllegalArgumentException("Invalid route.");
        }

        final FeatureType featureType = FeatureType.from(request.param(FEATURE_TYPE));
        final List<String> attributeParams = request.params()
            .keySet()
            .stream()
            .filter(key -> featureType.getAllowedAttributesRegistry().containsKey(key))
            .toList();
        for (String attributeName : attributeParams) {
            Attribute attribute = featureType.getAttributeFromName(attributeName);
            if (attribute == null) {
                throw new IllegalArgumentException(attributeName + " is not a valid attribute under feature type " + featureType.getName());
            }
            attributeFilters.put(attribute, parseAttributeValues(request.param(attributeName), attributeName, featureType));
        }
        final GetRuleRequest getRuleRequest = new GetRuleRequest(
            request.param(ID_STRING),
            attributeFilters,
            request.param(SEARCH_AFTER_STRING),
            featureType
        );
        return channel -> client.execute(GetRuleAction.INSTANCE, getRuleRequest, getRuleResponse(channel));
    }

    /**
     * Parses a comma-separated string of attribute values and validates each value.
     * @param attributeValues - the comma-separated string representing attributeValues
     * @param attributeName - attribute name
     */
    private HashSet<String> parseAttributeValues(String attributeValues, String attributeName, FeatureType featureType) {
        String[] valuesArray = attributeValues.split(",");
        int maxNumberOfValuesPerAttribute = featureType.getMaxNumberOfValuesPerAttribute();
        if (valuesArray.length > maxNumberOfValuesPerAttribute) {
            throw new IllegalArgumentException(
                "The attribute value length for " + attributeName + " exceeds the maximum allowed of " + maxNumberOfValuesPerAttribute
            );
        }
        for (String value : valuesArray) {
            if (value == null || value.trim().isEmpty() || value.length() > featureType.getMaxCharLengthPerAttributeValue()) {
                throw new IllegalArgumentException(
                    "Invalid attribute value for: "
                        + attributeName
                        + " : String cannot be empty or over "
                        + featureType.getMaxCharLengthPerAttributeValue()
                        + " characters."
                );
            }
        }
        return new HashSet<>(Arrays.asList(valuesArray));
    }

    private RestResponseListener<GetRuleResponse> getRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
