/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.document;

import org.density.Version;
import org.density.action.bulk.BulkRequest;
import org.density.action.bulk.BulkResponse;
import org.density.common.SetOnce;
import org.density.common.xcontent.XContentType;
import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesArray;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.rest.RestChannel;
import org.density.rest.RestRequest;
import org.density.rest.RestResponse;
import org.density.test.DensityTestCase;
import org.density.test.client.NoOpNodeClient;
import org.density.test.rest.FakeRestRequest;
import org.density.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RestBulkStreamingAction}.
 */
public class RestBulkStreamingActionTests extends DensityTestCase {
    public void testBulkStreamingPipelineUpsert() throws Exception {
        SetOnce<Boolean> bulkCalled = new SetOnce<>();
        try (NodeClient verifyingClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
                bulkCalled.set(true);
            }
        }) {
            final Map<String, String> params = new HashMap<>();
            params.put("pipeline", "timestamps");

            final FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("my_index/_bulk/streaming")
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
                .build();
            request.param("error_trace", "false");
            request.param("rest.exception.stacktrace.skip", "false");

            final RestChannel channel = mock(RestChannel.class);
            when(channel.request()).thenReturn(request);
            when(channel.newErrorBuilder()).thenReturn(XContentType.YAML.contentBuilder());
            when(channel.detailedErrorsEnabled()).thenReturn(true);

            new RestBulkStreamingAction(settings(Version.CURRENT).build()).handleRequest(request, channel, verifyingClient);

            final ArgumentCaptor<RestResponse> responseCaptor = ArgumentCaptor.captor();
            verify(channel).sendResponse(responseCaptor.capture());

            // We do not expect `bulk` action to be called since the default HTTP transport (netty4) does not support streaming
            assertThat(bulkCalled.get(), equalTo(null));
            assertThat(responseCaptor.getValue().status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(
                responseCaptor.getValue().content().utf8ToString(),
                containsString("Unable to initiate request / response streaming")
            );
        }
    }
}
