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

package org.density.action.admin.cluster.storedscripts;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.xcontent.XContentType;
import org.density.core.common.bytes.BytesArray;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.script.StoredScriptSource;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Collections;

public class PutStoredScriptRequestTests extends DensityTestCase {

    public void testSerialization() throws IOException {
        PutStoredScriptRequest storedScriptRequest = new PutStoredScriptRequest(
            "bar",
            "context",
            new BytesArray("{}"),
            MediaTypeRegistry.JSON,
            new StoredScriptSource("foo", "bar", Collections.emptyMap())
        );

        assertEquals(MediaTypeRegistry.JSON, storedScriptRequest.mediaType());
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            storedScriptRequest.writeTo(output);

            try (StreamInput in = output.bytes().streamInput()) {
                PutStoredScriptRequest serialized = new PutStoredScriptRequest(in);
                assertEquals(MediaTypeRegistry.JSON, serialized.mediaType());
                assertEquals(storedScriptRequest.id(), serialized.id());
                assertEquals(storedScriptRequest.context(), serialized.context());
            }
        }
    }

    public void testToXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentBuilder.builder(xContentType.xContent());
        builder.startObject();
        builder.startObject("script").field("lang", "painless").field("source", "Math.log(_score * 2) + params.multiplier").endObject();
        builder.endObject();

        BytesReference expectedRequestBody = BytesReference.bytes(builder);

        PutStoredScriptRequest request = new PutStoredScriptRequest();
        request.id("test1");
        request.content(expectedRequestBody, xContentType);

        XContentBuilder requestBuilder = XContentBuilder.builder(xContentType.xContent());
        requestBuilder.startObject();
        request.toXContent(requestBuilder, ToXContent.EMPTY_PARAMS);
        requestBuilder.endObject();

        BytesReference actualRequestBody = BytesReference.bytes(requestBuilder);

        assertEquals(expectedRequestBody, actualRequestBody);
    }
}
