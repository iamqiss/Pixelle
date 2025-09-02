/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions.rest;

import org.density.DensityParseException;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.settings.Settings;
import org.density.core.common.bytes.BytesArray;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.BytesStreamInput;
import org.density.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.MediaType;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.core.xcontent.XContentParser;
import org.density.http.HttpRequest;
import org.density.identity.IdentityService;
import org.density.identity.Subject;
import org.density.identity.tokens.OnBehalfOfClaims;
import org.density.identity.tokens.TokenManager;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestRequest.Method;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.mockito.Mockito.mock;

public class ExtensionRestRequestTests extends DensityTestCase {

    private Method expectedMethod;
    private String expectedPath;
    private String expectedUri;
    Map<String, String> expectedParams;
    Map<String, List<String>> expectedHeaders;
    MediaType expectedContentType;
    BytesReference expectedContent;
    String extensionUniqueId1;
    Principal userPrincipal;
    HttpRequest.HttpVersion expectedHttpVersion;
    String extensionTokenProcessor;
    String expectedRequestIssuerIdentity;
    NamedWriteableRegistry registry;
    private IdentityService identityService;

    public void setUp() throws Exception {
        super.setUp();
        expectedMethod = Method.GET;
        expectedPath = "/test/uri";
        expectedUri = "foobar?foo=bar&baz=42";
        expectedParams = Map.ofEntries(entry("foo", "bar"), entry("baz", "42"));
        expectedHeaders = Map.ofEntries(
            entry("Content-Type", Arrays.asList("application/json")),
            entry("foo", Arrays.asList("hello", "world"))
        );
        expectedContentType = MediaTypeRegistry.JSON;
        expectedContent = new BytesArray("{\"key\": \"value\"}".getBytes(StandardCharsets.UTF_8));
        extensionUniqueId1 = "ext_1";
        userPrincipal = () -> "user1";
        expectedHttpVersion = HttpRequest.HttpVersion.HTTP_1_1;
        extensionTokenProcessor = "placeholder_extension_token_processor";
        identityService = new IdentityService(Settings.EMPTY, mock(ThreadPool.class), List.of());
        TokenManager tokenManager = identityService.getTokenManager();
        Subject subject = this.identityService.getCurrentSubject();
        OnBehalfOfClaims claims = new OnBehalfOfClaims("testID", subject.getPrincipal().getName());
        expectedRequestIssuerIdentity = identityService.getTokenManager()
            .issueOnBehalfOfToken(identityService.getCurrentSubject(), claims)
            .asAuthHeaderValue();
    }

    public void testExtensionRestRequest() throws Exception {
        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedUri,
            expectedPath,
            expectedParams,
            expectedHeaders,
            expectedContentType,
            expectedContent,
            expectedRequestIssuerIdentity,
            expectedHttpVersion
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedUri, request.uri());
        assertEquals(expectedPath, request.path());

        assertEquals(expectedParams, request.params());
        assertEquals(expectedHttpVersion, request.protocolVersion());

        assertEquals(Collections.emptyList(), request.consumedParams());
        assertTrue(request.hasParam("foo"));
        assertFalse(request.hasParam("bar"));
        assertEquals("bar", request.param("foo"));
        assertEquals("baz", request.param("bar", "baz"));
        assertEquals(42L, request.paramAsLong("baz", 0L));
        assertEquals(0L, request.paramAsLong("bar", 0L));
        assertTrue(request.consumedParams().contains("foo"));
        assertTrue(request.consumedParams().contains("baz"));

        assertEquals(expectedContentType, request.getXContentType());
        assertTrue(request.hasContent());
        assertFalse(request.isContentConsumed());
        assertEquals(expectedContent, request.content());
        assertTrue(request.isContentConsumed());

        XContentParser parser = request.contentParser(NamedXContentRegistry.EMPTY);
        Map<String, String> contentMap = parser.mapStrings();
        assertEquals("value", contentMap.get("key"));

        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedUri, request.uri());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertEquals(expectedHeaders, request.headers());
                    assertEquals(expectedContent, request.content());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                    assertEquals(expectedHttpVersion, request.protocolVersion());
                }
            }
        }
    }

    public void testExtensionRestRequestWithNoContent() throws Exception {
        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedUri,
            expectedPath,
            expectedParams,
            expectedHeaders,
            null,
            new BytesArray(new byte[0]),
            expectedRequestIssuerIdentity,
            expectedHttpVersion
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedPath, request.path());
        assertEquals(expectedParams, request.params());
        assertEquals(expectedHeaders, request.headers());
        assertNull(request.getXContentType());
        assertEquals(0, request.content().length());
        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
        assertEquals(expectedHttpVersion, request.protocolVersion());

        final ExtensionRestRequest requestWithNoContent = request;
        assertThrows(DensityParseException.class, () -> requestWithNoContent.contentParser(NamedXContentRegistry.EMPTY));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedUri, request.uri());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertNull(request.getXContentType());
                    assertEquals(0, request.content().length());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                    assertEquals(expectedHttpVersion, request.protocolVersion());

                    final ExtensionRestRequest requestWithNoContentType = request;
                    assertThrows(DensityParseException.class, () -> requestWithNoContentType.contentParser(NamedXContentRegistry.EMPTY));
                }
            }
        }
    }

    public void testExtensionRestRequestWithPlainTextContent() throws Exception {
        BytesReference expectedText = new BytesArray("Plain text");

        ExtensionRestRequest request = new ExtensionRestRequest(
            expectedMethod,
            expectedUri,
            expectedPath,
            expectedParams,
            expectedHeaders,
            null,
            expectedText,
            expectedRequestIssuerIdentity,
            expectedHttpVersion
        );

        assertEquals(expectedMethod, request.method());
        assertEquals(expectedUri, request.uri());
        assertEquals(expectedPath, request.path());
        assertEquals(expectedParams, request.params());
        assertNull(request.getXContentType());
        assertEquals(expectedText, request.content());
        assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
        assertEquals(expectedHttpVersion, request.protocolVersion());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                try (NamedWriteableAwareStreamInput nameWritableAwareIn = new NamedWriteableAwareStreamInput(in, registry)) {
                    request = new ExtensionRestRequest(nameWritableAwareIn);
                    assertEquals(expectedMethod, request.method());
                    assertEquals(expectedUri, request.uri());
                    assertEquals(expectedPath, request.path());
                    assertEquals(expectedParams, request.params());
                    assertNull(request.getXContentType());
                    assertEquals(expectedText, request.content());
                    assertEquals(expectedRequestIssuerIdentity, request.getRequestIssuerIdentity());
                    assertEquals(expectedHttpVersion, request.protocolVersion());
                }
            }
        }
    }

    public void testRestExecuteOnExtensionResponse() throws Exception {
        RestStatus expectedStatus = RestStatus.OK;
        String expectedContentType = BytesRestResponse.TEXT_CONTENT_TYPE;
        String expectedResponse = "Test response";
        byte[] expectedResponseBytes = expectedResponse.getBytes(StandardCharsets.UTF_8);

        RestExecuteOnExtensionResponse response = new RestExecuteOnExtensionResponse(
            expectedStatus,
            expectedContentType,
            expectedResponseBytes,
            Collections.emptyMap(),
            Collections.emptyList(),
            false
        );

        assertEquals(expectedStatus, response.getStatus());
        assertEquals(expectedContentType, response.getContentType());
        assertArrayEquals(expectedResponseBytes, response.getContent());
        assertEquals(0, response.getHeaders().size());
        assertEquals(0, response.getConsumedParams().size());
        assertFalse(response.isContentConsumed());

        String headerKey = "foo";
        List<String> headerValueList = List.of("bar", "baz");
        Map<String, List<String>> expectedHeaders = Map.of(headerKey, headerValueList);
        List<String> expectedConsumedParams = List.of("foo", "bar");

        response = new RestExecuteOnExtensionResponse(
            expectedStatus,
            expectedContentType,
            expectedResponseBytes,
            expectedHeaders,
            expectedConsumedParams,
            true
        );

        assertEquals(expectedStatus, response.getStatus());
        assertEquals(expectedContentType, response.getContentType());
        assertArrayEquals(expectedResponseBytes, response.getContent());

        assertEquals(1, response.getHeaders().keySet().size());
        assertTrue(response.getHeaders().containsKey(headerKey));

        List<String> fooList = response.getHeaders().get(headerKey);
        assertEquals(2, fooList.size());
        assertTrue(fooList.containsAll(headerValueList));

        assertEquals(2, response.getConsumedParams().size());
        assertTrue(response.getConsumedParams().containsAll(expectedConsumedParams));
        assertTrue(response.isContentConsumed());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                response = new RestExecuteOnExtensionResponse(in);

                assertEquals(expectedStatus, response.getStatus());
                assertEquals(expectedContentType, response.getContentType());
                assertArrayEquals(expectedResponseBytes, response.getContent());

                assertEquals(1, response.getHeaders().keySet().size());
                assertTrue(response.getHeaders().containsKey(headerKey));

                fooList = response.getHeaders().get(headerKey);
                assertEquals(2, fooList.size());
                assertTrue(fooList.containsAll(headerValueList));

                assertEquals(2, response.getConsumedParams().size());
                assertTrue(response.getConsumedParams().containsAll(expectedConsumedParams));
                assertTrue(response.isContentConsumed());
            }
        }
    }
}
