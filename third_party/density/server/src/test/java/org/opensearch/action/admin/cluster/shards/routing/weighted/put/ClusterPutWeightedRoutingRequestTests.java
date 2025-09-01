/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.shards.routing.weighted.put;

import org.density.action.ActionRequestValidationException;
import org.density.cluster.routing.WeightedRouting;
import org.density.core.common.bytes.BytesArray;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.test.DensityTestCase;

import java.util.Map;

public class ClusterPutWeightedRoutingRequestTests extends DensityTestCase {

    public void testSetWeightedRoutingWeight() {
        String reqString = "{\"weights\":{\"us-east-1c\":\"0\",\"us-east-1b\":\"1\",\"us-east-1a\":\"1\"},\"_version\":1}";
        ClusterPutWeightedRoutingRequest request = new ClusterPutWeightedRoutingRequest("zone");
        Map<String, Double> weights = Map.of("us-east-1a", 1.0, "us-east-1b", 1.0, "us-east-1c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);

        request.setWeightedRouting(new BytesArray(reqString), MediaTypeRegistry.JSON);
        assertEquals(weightedRouting, request.getWeightedRouting());
        assertEquals(1, request.getVersion());
    }

    public void testValidate_ValuesAreProper() {
        String reqString = "{\"weights\":{\"us-east-1c\":\"0\",\"us-east-1b\":\"1\",\"us-east-1a\":\"1\"},\"_version\":1}";
        ClusterPutWeightedRoutingRequest request = new ClusterPutWeightedRoutingRequest("zone");
        request.setWeightedRouting(new BytesArray(reqString), MediaTypeRegistry.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNull(actionRequestValidationException);
    }

    public void testValidate_MissingWeights() {
        String reqString = "{}";
        ClusterPutWeightedRoutingRequest request = new ClusterPutWeightedRoutingRequest("zone");
        request.setWeightedRouting(new BytesArray(reqString), MediaTypeRegistry.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("Weights are missing"));
    }

    public void testValidate_AttributeMissing() {
        String reqString = "{\"weights\":{\"us-east-1c\":\"0\",\"us-east-1b\":\"1\",\"us-east-1a\": \"1\"},\"_version\":1}";
        ClusterPutWeightedRoutingRequest request = new ClusterPutWeightedRoutingRequest();
        request.setWeightedRouting(new BytesArray(reqString), MediaTypeRegistry.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("Attribute name is missing"));
    }

    public void testValidate_MoreThanHalfWithZeroWeight() {
        String reqString = "{\"weights\":{\"us-east-1c\":\"0\",\"us-east-1b\":\"0\",\"us-east-1a\": \"1\"}," + "\"_version\":1}";
        ClusterPutWeightedRoutingRequest request = new ClusterPutWeightedRoutingRequest("zone");
        request.setWeightedRouting(new BytesArray(reqString), MediaTypeRegistry.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(
            actionRequestValidationException.getMessage().contains("Maximum expected number of routing weights having zero weight is [1]")
        );
    }

    public void testValidate_VersionMissing() {
        String reqString = "{\"weights\":{\"us-east-1c\": \"0\",\"us-east-1b\": \"1\",\"us-east-1a\": \"1\"}}";
        ClusterPutWeightedRoutingRequest request = new ClusterPutWeightedRoutingRequest("zone");
        request.setWeightedRouting(new BytesArray(reqString), MediaTypeRegistry.JSON);
        ActionRequestValidationException actionRequestValidationException = request.validate();
        assertNotNull(actionRequestValidationException);
        assertTrue(actionRequestValidationException.getMessage().contains("Version is missing"));
    }
}
