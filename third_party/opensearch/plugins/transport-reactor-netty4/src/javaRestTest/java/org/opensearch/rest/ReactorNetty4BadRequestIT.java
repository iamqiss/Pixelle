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

package org.density.rest;

import org.density.client.Request;
import org.density.client.RequestOptions;
import org.density.client.Response;
import org.density.client.ResponseException;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.core.common.unit.ByteSizeValue;
import org.density.http.HttpTransportSettings;
import org.density.test.rest.DensityRestTestCase;
import org.density.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import static org.density.core.rest.RestStatus.REQUEST_URI_TOO_LONG;
import static org.hamcrest.Matchers.equalTo;

public class ReactorNetty4BadRequestIT extends DensityRestTestCase {

    public void testBadRequest() throws IOException {
        final Response response = client().performRequest(new Request("GET", "/_nodes/settings"));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> map = objectPath.evaluate("nodes");
        int maxMaxInitialLineLength = Integer.MIN_VALUE;
        final Setting<ByteSizeValue> httpMaxInitialLineLength = HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
        final String key = httpMaxInitialLineLength.getKey().substring("http.".length());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) entry.getValue()).get("settings");
            final int maxIntialLineLength;
            if (settings.containsKey("http")) {
                @SuppressWarnings("unchecked")
                final Map<String, Object> httpSettings = (Map<String, Object>) settings.get("http");
                if (httpSettings.containsKey(key)) {
                    maxIntialLineLength = ByteSizeValue.parseBytesSizeValue((String) httpSettings.get(key), key).bytesAsInt();
                } else {
                    maxIntialLineLength = httpMaxInitialLineLength.getDefault(Settings.EMPTY).bytesAsInt();
                }
            } else {
                maxIntialLineLength = httpMaxInitialLineLength.getDefault(Settings.EMPTY).bytesAsInt();
            }
            maxMaxInitialLineLength = Math.max(maxMaxInitialLineLength, maxIntialLineLength);
        }

        final String path = "/" + new String(new byte[maxMaxInitialLineLength], Charset.forName("UTF-8")).replace('\0', 'a');
        final ResponseException e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request(randomFrom("GET", "POST", "PUT"), path))
        );
        // The reactor-netty implementation does not provide a hook to customize or intercept request decoder errors at the moment (see
        // please https://github.com/reactor/reactor-netty/issues/3327).
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(REQUEST_URI_TOO_LONG.getStatus()));
    }

    public void testInvalidParameterValue() throws IOException {
        final Request request = new Request("GET", "/_cluster/settings");
        request.addParameter("pretty", "neither-true-nor-false");
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        final Response response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> map = objectPath.evaluate("error");
        assertThat(map.get("type"), equalTo("illegal_argument_exception"));
        assertThat(map.get("reason"), equalTo("Failed to parse value [neither-true-nor-false] as only [true] or [false] are allowed."));
    }

    public void testInvalidHeaderValue() throws IOException {
        final Request request = new Request("GET", "/_cluster/settings");
        final RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader("Content-Type", "\t");
        request.setOptions(options);
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        final Response response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(400));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> map = objectPath.evaluate("error");
        assertThat(map.get("type"), equalTo("content_type_header_exception"));
        assertThat(map.get("reason"), equalTo("java.lang.IllegalArgumentException: invalid Content-Type header []"));
    }

    public void testUnsupportedContentType() throws IOException {
        final Request request = new Request("POST", "/_bulk/stream");
        final RequestOptions.Builder options = request.getOptions().toBuilder();
        request.setOptions(options);
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        final Response response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), equalTo(406));
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final String error = objectPath.evaluate("error");
        assertThat(error, equalTo("Content-Type header [] is not supported"));
    }
}
