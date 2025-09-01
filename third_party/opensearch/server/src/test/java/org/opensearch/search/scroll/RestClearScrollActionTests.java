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

package org.density.search.scroll;

import org.density.action.search.ClearScrollRequest;
import org.density.action.search.ClearScrollResponse;
import org.density.common.SetOnce;
import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesArray;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.rest.RestRequest;
import org.density.rest.action.search.RestClearScrollAction;
import org.density.test.DensityTestCase;
import org.density.test.client.NoOpNodeClient;
import org.density.test.rest.FakeRestChannel;
import org.density.test.rest.FakeRestRequest;
import org.density.transport.client.node.NodeClient;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class RestClearScrollActionTests extends DensityTestCase {

    public void testParseClearScrollRequestWithInvalidJsonThrowsException() throws Exception {
        RestClearScrollAction action = new RestClearScrollAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            MediaTypeRegistry.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testBodyParamsOverrideQueryStringParams() throws Exception {
        SetOnce<Boolean> scrollCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void clearScroll(ClearScrollRequest request, ActionListener<ClearScrollResponse> listener) {
                scrollCalled.set(true);
                assertThat(request.getScrollIds(), hasSize(1));
                assertThat(request.getScrollIds().get(0), equalTo("BODY"));
            }
        }) {
            RestClearScrollAction action = new RestClearScrollAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(
                Collections.singletonMap("scroll_id", "QUERY_STRING")
            ).withContent(new BytesArray("{\"scroll_id\": [\"BODY\"]}"), MediaTypeRegistry.JSON).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(scrollCalled.get(), equalTo(true));
        }
    }
}
