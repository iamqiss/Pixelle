/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.common.annotation.ExperimentalApi;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.ToXContentObject;
import org.density.core.xcontent.XContentBuilder;
import org.density.rule.autotagging.Rule;

import java.io.IOException;

/**
 * Response for the update API for Rule
 * Example response:
 * {
 *     id": "z1MJApUB0zgMcDmz-UQq",
 *     "description": "Rule for tagging workload_group_id to index123"
 *     "index_pattern": ["index123"],
 *     "workload_group": "workload_group_id",
 *     "updated_at": "2025-02-14T01:19:22.589Z"
 * }
 * @density.experimental
 */
@ExperimentalApi
public class UpdateRuleResponse extends ActionResponse implements ToXContent, ToXContentObject {
    private final Rule rule;

    /**
     * constructor for UpdateRuleResponse
     * @param rule - the updated rule
     */
    public UpdateRuleResponse(final Rule rule) {
        this.rule = rule;
    }

    /**
     * Constructs a UpdateRuleResponse from a StreamInput for deserialization
     * @param in - The {@link StreamInput} instance to read from.
     */
    public UpdateRuleResponse(StreamInput in) throws IOException {
        this(new Rule(in));
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
    Rule getRule() {
        return rule;
    }
}
