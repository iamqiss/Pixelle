/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.autotagging;

import org.density.ResourceNotFoundException;
import org.density.rule.utils.RuleTestUtils;
import org.density.test.DensityTestCase;
import org.junit.BeforeClass;

import static org.density.rule.autotagging.AutoTaggingRegistry.MAX_FEATURE_TYPE_NAME_LENGTH;
import static org.density.rule.autotagging.RuleTests.INVALID_FEATURE;
import static org.density.rule.utils.RuleTestUtils.FEATURE_TYPE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoTaggingRegistryTests extends DensityTestCase {

    @BeforeClass
    public static void setUpOnce() {
        FeatureType featureType = RuleTestUtils.MockRuleFeatureType.INSTANCE;
        AutoTaggingRegistry.registerFeatureType(featureType);
    }

    public void testGetFeatureType_Success() {
        FeatureType retrievedFeatureType = AutoTaggingRegistry.getFeatureType(FEATURE_TYPE_NAME);
        assertEquals(FEATURE_TYPE_NAME, retrievedFeatureType.getName());
    }

    public void testRuntimeException() {
        assertThrows(ResourceNotFoundException.class, () -> AutoTaggingRegistry.getFeatureType(INVALID_FEATURE));
    }

    public void testIllegalStateExceptionException() {
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(null));
        FeatureType featureType = mock(FeatureType.class);
        when(featureType.getName()).thenReturn(FEATURE_TYPE_NAME);
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(featureType));
        when(featureType.getName()).thenReturn(randomAlphaOfLength(MAX_FEATURE_TYPE_NAME_LENGTH + 1));
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(featureType));
    }
}
