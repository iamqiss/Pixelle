/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.qa.smoketest;

import org.density.client.Request;
import org.density.common.xcontent.XContentHelper;
import org.density.common.xcontent.XContentType;
import org.density.test.rest.DensityRestTestCase;

import java.io.InputStream;
import java.util.Map;

public class FipsSmokeRandomnessIT extends DensityRestTestCase {

    public void testGetRandomness() throws Exception {
        var response = client().performRequest(new Request("GET", "/_randomness/provider_name"));
        assertEquals(200, response.getStatusLine().getStatusCode());
        try (InputStream is = response.getEntity().getContent()) {
            Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, false);
            assertEquals("BCFIPS_RNG", map.get("name"));
        }
    }
}
