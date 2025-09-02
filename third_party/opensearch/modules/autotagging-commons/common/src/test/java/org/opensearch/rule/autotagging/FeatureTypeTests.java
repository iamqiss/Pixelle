/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.autotagging;

import org.density.core.common.io.stream.StreamOutput;
import org.density.test.DensityTestCase;

import java.io.IOException;

import static org.density.rule.autotagging.RuleTests.FEATURE_TYPE;
import static org.density.rule.autotagging.RuleTests.INVALID_ATTRIBUTE;
import static org.density.rule.autotagging.RuleTests.TEST_ATTR1_NAME;
import static org.density.rule.autotagging.RuleTests.TestAttribute.TEST_ATTRIBUTE_1;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FeatureTypeTests extends DensityTestCase {
    public void testIsValidAttribute() {
        assertTrue(FEATURE_TYPE.isValidAttribute(TEST_ATTRIBUTE_1));
        assertFalse(FEATURE_TYPE.isValidAttribute(mock(Attribute.class)));
    }

    public void testGetAttributeFromName() {
        assertEquals(TEST_ATTRIBUTE_1, FEATURE_TYPE.getAttributeFromName(TEST_ATTR1_NAME));
        assertNull(FEATURE_TYPE.getAttributeFromName(INVALID_ATTRIBUTE));
    }

    public void testWriteTo() throws IOException {
        StreamOutput mockOutput = mock(StreamOutput.class);
        FEATURE_TYPE.writeTo(mockOutput);
        verify(mockOutput).writeString(anyString());
    }
}
