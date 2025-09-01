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

package org.density.search;

import org.density.action.search.ClearScrollResponse;
import org.density.common.xcontent.XContentType;
import org.density.common.xcontent.json.JsonXContent;
import org.density.core.common.bytes.BytesReference;
import org.density.core.xcontent.MediaType;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentHelper;
import org.density.core.xcontent.XContentParser;
import org.density.test.DensityTestCase;

import java.io.IOException;

import static org.density.test.hamcrest.DensityAssertions.assertToXContentEquivalent;

public class ClearScrollResponseTests extends DensityTestCase {

    public void testToXContent() throws IOException {
        ClearScrollResponse clearScrollResponse = new ClearScrollResponse(true, 10);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            clearScrollResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        assertEquals(true, clearScrollResponse.isSucceeded());
        assertEquals(10, clearScrollResponse.getNumFreed());
    }

    public void testToAndFromXContent() throws IOException {
        MediaType mediaType = randomFrom(XContentType.values());
        ClearScrollResponse originalResponse = createTestItem();
        BytesReference originalBytes = toShuffledXContent(originalResponse, mediaType, ToXContent.EMPTY_PARAMS, randomBoolean());
        ClearScrollResponse parsedResponse;
        try (XContentParser parser = createParser(mediaType.xContent(), originalBytes)) {
            parsedResponse = ClearScrollResponse.fromXContent(parser);
        }
        assertEquals(originalResponse.isSucceeded(), parsedResponse.isSucceeded());
        assertEquals(originalResponse.getNumFreed(), parsedResponse.getNumFreed());
        BytesReference parsedBytes = XContentHelper.toXContent(parsedResponse, mediaType, randomBoolean());
        assertToXContentEquivalent(originalBytes, parsedBytes, mediaType);
    }

    private static ClearScrollResponse createTestItem() {
        return new ClearScrollResponse(randomBoolean(), randomIntBetween(0, Integer.MAX_VALUE));
    }
}
