/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.backwards;

import org.apache.hc.core5.http.HttpHost;
import org.density.action.get.MultiGetRequest;
import org.density.action.get.MultiGetResponse;
import org.density.client.Request;
import org.density.client.RequestOptions;
import org.density.client.Response;
import org.density.client.RestClient;
import org.density.client.RestHighLevelClient;
import org.density.test.rest.DensityRestTestCase;
import org.density.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class SearchingIT extends DensityRestTestCase {
    public void testMultiGet() throws Exception {
        final Set<HttpHost> nodes = buildNodes();

        final MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.add("index", "id1");

        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(nodes.toArray(HttpHost[]::new)))) {
            MultiGetResponse response = client.mget(multiGetRequest, RequestOptions.DEFAULT);
            assertEquals(1, response.getResponses().length);

            assertTrue(response.getResponses()[0].isFailed());
            assertNotNull(response.getResponses()[0].getFailure());
            assertEquals(response.getResponses()[0].getFailure().getId(), "id1");
            assertEquals(response.getResponses()[0].getFailure().getIndex(), "index");
            assertThat(response.getResponses()[0].getFailure().getMessage(), containsString("no such index [index]"));
       }
    }

    private Set<HttpHost> buildNodes() throws IOException, URISyntaxException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        final Set<HttpHost> nodes = new HashSet<>();
        for (String id : nodesAsMap.keySet()) {
            nodes.add(HttpHost.create((String) objectPath.evaluate("nodes." + id + ".http.publish_address")));
        }

        return nodes;
    }
}
