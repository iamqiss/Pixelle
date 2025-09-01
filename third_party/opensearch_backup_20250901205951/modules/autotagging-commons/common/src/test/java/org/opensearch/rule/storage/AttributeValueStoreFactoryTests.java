/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.storage;

import org.density.rule.autotagging.Attribute;
import org.density.rule.utils.RuleTestUtils.MockRuleAttributes;
import org.density.rule.utils.RuleTestUtils.MockRuleFeatureType;
import org.density.test.DensityTestCase;

public class AttributeValueStoreFactoryTests extends DensityTestCase {
    AttributeValueStoreFactory sut;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        sut = new AttributeValueStoreFactory(MockRuleFeatureType.INSTANCE, DefaultAttributeValueStore::new);
    }

    public void testFeatureLevelStoreInitialisation() {
        for (Attribute attribute : MockRuleFeatureType.INSTANCE.getAllowedAttributesRegistry().values()) {
            assertTrue(sut.getAttributeValueStore(attribute) instanceof DefaultAttributeValueStore<String, String>);
        }
    }

    public void testValidGetAttributeValueStore() {
        assertTrue(
            sut.getAttributeValueStore(MockRuleAttributes.MOCK_RULE_ATTRIBUTE_ONE) instanceof DefaultAttributeValueStore<String, String>
        );
    }

    public void testInValidGetAttributeValueStore() {
        assertThrows(IllegalArgumentException.class, () -> { sut.getAttributeValueStore(MockRuleAttributes.INVALID_ATTRIBUTE); });
    }
}
