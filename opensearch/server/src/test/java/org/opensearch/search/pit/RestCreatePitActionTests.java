/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pit;

import org.density.action.search.CreatePitRequest;
import org.density.action.search.CreatePitResponse;
import org.density.common.SetOnce;
import org.density.core.action.ActionListener;
import org.density.rest.RestRequest;
import org.density.rest.action.search.RestCreatePitAction;
import org.density.test.DensityTestCase;
import org.density.test.client.NoOpNodeClient;
import org.density.test.rest.FakeRestChannel;
import org.density.test.rest.FakeRestRequest;
import org.density.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify behavior of create pit rest action
 */
public class RestCreatePitActionTests extends DensityTestCase {
    public void testRestCreatePit() throws Exception {
        SetOnce<Boolean> createPitCalled = new SetOnce<>();
        RestCreatePitAction action = new RestCreatePitAction();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void createPit(CreatePitRequest request, ActionListener<CreatePitResponse> listener) {
                createPitCalled.set(true);
                assertThat(request.getKeepAlive().getStringRep(), equalTo("1m"));
                assertFalse(request.shouldAllowPartialPitCreation());
            }
        }) {
            Map<String, String> params = new HashMap<>();
            params.put("keep_alive", "1m");
            params.put("allow_partial_pit_creation", "false");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
                .withMethod(RestRequest.Method.POST)
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(createPitCalled.get(), equalTo(true));
        }
    }

    public void testRestCreatePitDefaultPartialCreation() throws Exception {
        SetOnce<Boolean> createPitCalled = new SetOnce<>();
        RestCreatePitAction action = new RestCreatePitAction();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void createPit(CreatePitRequest request, ActionListener<CreatePitResponse> listener) {
                createPitCalled.set(true);
                assertThat(request.getKeepAlive().getStringRep(), equalTo("1m"));
                assertTrue(request.shouldAllowPartialPitCreation());
            }
        }) {
            Map<String, String> params = new HashMap<>();
            params.put("keep_alive", "1m");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
                .withMethod(RestRequest.Method.POST)
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(createPitCalled.get(), equalTo(true));
        }
    }
}
