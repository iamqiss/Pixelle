/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

import java.io.IOException;

import static org.density.rule.utils.RuleTestUtils.assertEqualRule;
import static org.density.rule.utils.RuleTestUtils.ruleOne;

public class CreateRuleRequestTests extends DensityTestCase {

    /**
     * Test case to verify the serialization and deserialization of CreateRuleRequest.
     */
    public void testSerialization() throws IOException {
        CreateRuleRequest request = new CreateRuleRequest(ruleOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateRuleRequest otherRequest = new CreateRuleRequest(streamInput);
        assertEqualRule(ruleOne, otherRequest.getRule(), false);
    }
}
