/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rule;

import org.density.cluster.service.ClusterService;
import org.density.rule.RuleAttribute;
import org.density.rule.autotagging.Attribute;
import org.density.rule.autotagging.AutoTaggingRegistry;
import org.density.test.DensityTestCase;

import java.util.Map;

import static org.mockito.Mockito.mock;

public class WorkloadGroupFeatureTypeTests extends DensityTestCase {
    WorkloadGroupFeatureType featureType = new WorkloadGroupFeatureType(new WorkloadGroupFeatureValueValidator(mock(ClusterService.class)));

    public void testGetName_returnsCorrectName() {
        assertEquals("workload_group", featureType.getName());
    }

    public void testMaxNumberOfValuesPerAttribute() {
        assertEquals(10, featureType.getMaxNumberOfValuesPerAttribute());
    }

    public void testMaxCharLengthPerAttributeValue() {
        assertEquals(100, featureType.getMaxCharLengthPerAttributeValue());
    }

    public void testGetAllowedAttributesRegistry_containsIndexPattern() {
        Map<String, Attribute> allowedAttributes = featureType.getAllowedAttributesRegistry();
        assertTrue(allowedAttributes.containsKey("index_pattern"));
        assertEquals(RuleAttribute.INDEX_PATTERN, allowedAttributes.get("index_pattern"));
    }

    public void testRegisterFeatureType() {
        AutoTaggingRegistry.registerFeatureType(featureType);
    }
}
