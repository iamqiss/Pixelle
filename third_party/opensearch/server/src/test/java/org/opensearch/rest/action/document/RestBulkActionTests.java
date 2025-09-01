/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.rest.action.document;

import org.density.Version;
import org.density.action.bulk.BulkRequest;
import org.density.action.bulk.BulkResponse;
import org.density.action.update.UpdateRequest;
import org.density.common.SetOnce;
import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesArray;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.rest.RestChannel;
import org.density.rest.RestRequest;
import org.density.test.DensityTestCase;
import org.density.test.client.NoOpNodeClient;
import org.density.test.rest.FakeRestRequest;
import org.density.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link RestBulkAction}.
 */
public class RestBulkActionTests extends DensityTestCase {

    public void testBulkPipelineUpsert() throws Exception {
        SetOnce<Boolean> bulkCalled = new SetOnce<>();
        try (NodeClient verifyingClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
                bulkCalled.set(true);
                assertThat(request.requests(), hasSize(2));
                UpdateRequest updateRequest = (UpdateRequest) request.requests().get(1);
                assertThat(updateRequest.upsertRequest().getPipeline(), equalTo("timestamps"));
            }
        }) {
            final Map<String, String> params = new HashMap<>();
            params.put("pipeline", "timestamps");
            new RestBulkAction(settings(Version.CURRENT).build()).handleRequest(
                new FakeRestRequest.Builder(xContentRegistry()).withPath("my_index/_bulk")
                    .withParams(params)
                    .withContent(
                        new BytesArray(
                            "{\"index\":{\"_id\":\"1\"}}\n"
                                + "{\"field1\":\"val1\"}\n"
                                + "{\"update\":{\"_id\":\"2\"}}\n"
                                + "{\"script\":{\"source\":\"ctx._source.counter++;\"},\"upsert\":{\"field1\":\"upserted_val\"}}\n"
                        ),
                        MediaTypeRegistry.JSON
                    )
                    .withMethod(RestRequest.Method.POST)
                    .build(),
                mock(RestChannel.class),
                verifyingClient
            );
            assertThat(bulkCalled.get(), equalTo(true));
        }
    }
}
