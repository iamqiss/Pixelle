/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.network.NetworkAddress;
import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.transport.TransportAddress;
import org.density.core.rest.RestStatus;
import org.density.http.HttpRequest;
import org.density.http.HttpResponse;
import org.density.rest.RestRequest;
import org.density.telemetry.tracing.attributes.Attributes;
import org.density.telemetry.tracing.noop.NoopSpan;
import org.density.test.DensityTestCase;
import org.density.transport.Transport;
import org.density.transport.TransportException;
import org.density.transport.TransportRequest;
import org.density.transport.TransportRequestOptions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class SpanBuilderTests extends DensityTestCase {

    public String uri;

    public String expectedSpanName;

    public String expectedQueryParams;

    public String expectedReqRawPath;

    @ParametersFactory
    public static Collection<Object[]> data() {
        return Arrays.asList(
            new Object[][] {
                { "/_test/resource?name=John&age=25", "GET /_test/resource", "name=John&age=25", "/_test/resource" },
                { "/_test/", "GET /_test/", "", "/_test/" }, }
        );
    }

    public SpanBuilderTests(String uri, String expectedSpanName, String expectedQueryParams, String expectedReqRawPath) {
        this.uri = uri;
        this.expectedSpanName = expectedSpanName;
        this.expectedQueryParams = expectedQueryParams;
        this.expectedReqRawPath = expectedReqRawPath;
    }

    public void testHttpRequestContext() {
        HttpRequest httpRequest = createHttpRequest(uri);
        SpanCreationContext context = SpanBuilder.from(httpRequest);
        Attributes attributes = context.getAttributes();
        assertEquals(expectedSpanName, context.getSpanName());
        assertEquals("true", attributes.getAttributesMap().get(AttributeNames.TRACE));
        assertEquals("GET", attributes.getAttributesMap().get(AttributeNames.HTTP_METHOD));
        assertEquals("HTTP_1_0", attributes.getAttributesMap().get(AttributeNames.HTTP_PROTOCOL_VERSION));
        assertEquals(uri, attributes.getAttributesMap().get(AttributeNames.HTTP_URI));
        if (expectedQueryParams.isBlank()) {
            assertNull(attributes.getAttributesMap().get(AttributeNames.HTTP_REQ_QUERY_PARAMS));
        } else {
            assertEquals(expectedQueryParams, attributes.getAttributesMap().get(AttributeNames.HTTP_REQ_QUERY_PARAMS));
        }
    }

    public void testRestRequestContext() {
        RestRequest restRequest = RestRequest.request(null, createHttpRequest(uri), null);
        SpanCreationContext context = SpanBuilder.from(restRequest);
        Attributes attributes = context.getAttributes();
        assertEquals(expectedSpanName, context.getSpanName());
        assertEquals(expectedReqRawPath, attributes.getAttributesMap().get(AttributeNames.REST_REQ_RAW_PATH));
        assertNotNull(attributes.getAttributesMap().get(AttributeNames.REST_REQ_ID));
        if (expectedQueryParams.isBlank()) {
            assertNull(attributes.getAttributesMap().get(AttributeNames.HTTP_REQ_QUERY_PARAMS));
        } else {
            assertEquals(expectedQueryParams, attributes.getAttributesMap().get(AttributeNames.HTTP_REQ_QUERY_PARAMS));
        }
    }

    public void testRestRequestContextForNull() {
        SpanCreationContext context = SpanBuilder.from((RestRequest) null);
        assertEquals("rest_request", context.getSpanName());
        assertEquals(Attributes.EMPTY, context.getAttributes());
    }

    public void testTransportContext() {
        String action = "test-action";
        Transport.Connection connection = createTransportConnection();
        SpanCreationContext context = SpanBuilder.from(action, connection);
        Attributes attributes = context.getAttributes();
        assertEquals(action + " " + NetworkAddress.format(TransportAddress.META_ADDRESS), context.getSpanName());
        assertEquals(connection.getNode().getHostAddress(), attributes.getAttributesMap().get(AttributeNames.TRANSPORT_TARGET_HOST));
    }

    public void testParentSpan() {
        String spanName = "test-name";
        SpanContext parentSpanContext = new SpanContext(NoopSpan.INSTANCE);
        SpanCreationContext context = SpanBuilder.from(spanName, parentSpanContext);
        Attributes attributes = context.getAttributes();
        assertNull(attributes);
        assertEquals(spanName, context.getSpanName());
        assertEquals(parentSpanContext, context.getParent());
    }

    private static Transport.Connection createTransportConnection() {
        return new Transport.Connection() {
            @Override
            public DiscoveryNode getNode() {
                return new DiscoveryNode("local", new TransportAddress(TransportAddress.META_ADDRESS, 9200), Version.V_2_0_0);
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws IOException, TransportException {

            }

            @Override
            public void addCloseListener(ActionListener<Void> listener) {

            }

            @Override
            public boolean isClosed() {
                return false;
            }

            @Override
            public void close() {

            }
        };
    }

    private static HttpRequest createHttpRequest(String uri) {
        return new HttpRequest() {
            @Override
            public RestRequest.Method method() {
                return RestRequest.Method.GET;
            }

            @Override
            public String uri() {
                return uri;
            }

            @Override
            public BytesReference content() {
                return null;
            }

            @Override
            public Map<String, List<String>> getHeaders() {
                return Map.of("trace", Arrays.asList("true"));
            }

            @Override
            public List<String> strictCookies() {
                return null;
            }

            @Override
            public HttpVersion protocolVersion() {
                return HttpVersion.HTTP_1_0;
            }

            @Override
            public HttpRequest removeHeader(String header) {
                return null;
            }

            @Override
            public HttpResponse createResponse(RestStatus status, BytesReference content) {
                return null;
            }

            @Override
            public Exception getInboundException() {
                return null;
            }

            @Override
            public void release() {

            }

            @Override
            public HttpRequest releaseAndCopy() {
                return null;
            }
        };
    }
}
