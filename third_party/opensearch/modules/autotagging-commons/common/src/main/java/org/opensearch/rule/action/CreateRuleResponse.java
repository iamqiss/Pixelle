/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.ToXContentObject;
import org.density.core.xcontent.XContentBuilder;
import org.density.rule.autotagging.Rule;

import java.io.IOException;

/**
 * Response for the create API for Rule
 * Example response:
 * {
 *    "id":"wi6VApYBoX5wstmtU_8l",
 *    "description":"description1",
 *    "index_pattern":["log*", "uvent*"],
 *    "workload_group":"poOiU851RwyLYvV5lbvv5w",
 *    "updated_at":"2025-04-04T20:54:22.406Z"
 * }
 * @density.experimental
 */
public class CreateRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final Rule rule;

    /**
     * contructor for CreateRuleResponse
     * @param rule - the rule created
     */
    public CreateRuleResponse(final Rule rule) {
        this.rule = rule;
    }

    /**
     * Constructs a CreateRuleResponse from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public CreateRuleResponse(StreamInput in) throws IOException {
        rule = new Rule(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        rule.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return rule.toXContent(builder, params);
    }

    /**
     * rule getter
     */
    public Rule getRule() {
        return rule;
    }
}
