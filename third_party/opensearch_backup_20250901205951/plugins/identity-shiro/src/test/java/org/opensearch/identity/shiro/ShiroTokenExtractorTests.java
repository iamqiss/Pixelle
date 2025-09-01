/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.shiro;

import org.density.identity.tokens.AuthToken;
import org.density.identity.tokens.BasicAuthToken;
import org.density.rest.RestRequest;
import org.density.test.DensityTestCase;
import org.density.test.rest.FakeRestRequest;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ShiroTokenExtractorTests extends DensityTestCase {

    public void testAuthorizationHeaderExtractionWithBasicAuthToken() {
        String basicAuthHeader = Base64.getEncoder().encodeToString("foo:bar".getBytes(StandardCharsets.UTF_8));
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of(ShiroTokenExtractor.AUTH_HEADER_NAME, List.of(BasicAuthToken.TOKEN_IDENTIFIER + " " + basicAuthHeader))
        ).build();
        AuthToken extractedToken = ShiroTokenExtractor.extractToken(fakeRequest);
        assertThat(extractedToken, instanceOf(BasicAuthToken.class));
        assertThat(extractedToken.asAuthHeaderValue(), equalTo(basicAuthHeader));
    }

    public void testAuthorizationHeaderExtractionWithUnknownToken() {
        String authHeader = "foo";
        RestRequest fakeRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of(ShiroTokenExtractor.AUTH_HEADER_NAME, List.of(authHeader))
        ).build();
        AuthToken extractedToken = ShiroTokenExtractor.extractToken(fakeRequest);
        assertNull(extractedToken);
    }
}
