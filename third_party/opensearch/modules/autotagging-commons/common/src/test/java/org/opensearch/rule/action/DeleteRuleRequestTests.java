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
import org.density.rule.utils.RuleTestUtils;
import org.density.test.DensityTestCase;

import java.io.IOException;

import static org.density.rule.utils.RuleTestUtils._ID_ONE;

public class DeleteRuleRequestTests extends DensityTestCase {

    public void testSerialization() throws IOException {
        DeleteRuleRequest request = new DeleteRuleRequest(_ID_ONE, RuleTestUtils.MockRuleFeatureType.INSTANCE);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        DeleteRuleRequest deserialized = new DeleteRuleRequest(in);
        assertEquals(request.getRuleId(), deserialized.getRuleId());
        assertEquals(request.getFeatureType(), deserialized.getFeatureType());
    }

    public void testValidate_withMissingId() {
        DeleteRuleRequest request = new DeleteRuleRequest("", RuleTestUtils.MockRuleFeatureType.INSTANCE);
        assertNotNull(request.validate());
    }
}
