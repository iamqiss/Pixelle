/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.action.shard.routing.weighted.get;

import org.density.action.ActionRequestValidationException;
import org.density.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequest;
import org.density.test.DensityTestCase;

public class ClusterGetWeightedRoutingRequestTests extends DensityTestCase {

    public void testValidate_AwarenessAttributeIsSet() {
        ClusterGetWeightedRoutingRequest request = new ClusterGetWeightedRoutingRequest();
        request.setAwarenessAttribute("zone");
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNull(actionRequestValidationException);
    }

    public void testValidate_AwarenessAttributeNotSet() {
        ClusterGetWeightedRoutingRequest request = new ClusterGetWeightedRoutingRequest();
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("Awareness attribute is missing"));
    }

    public void testValidate_AwarenessAttributeIsEmpty() {
        ClusterGetWeightedRoutingRequest request = new ClusterGetWeightedRoutingRequest();
        request.setAwarenessAttribute("");
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("Awareness attribute is missing"));
    }

}
