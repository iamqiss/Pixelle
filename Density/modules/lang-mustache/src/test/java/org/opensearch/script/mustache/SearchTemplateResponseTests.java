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

package org.density.script.mustache;

import org.apache.lucene.search.TotalHits;
import org.density.action.search.SearchResponse;
import org.density.action.search.ShardSearchFailure;
import org.density.common.xcontent.XContentFactory;
import org.density.common.xcontent.XContentType;
import org.density.core.common.bytes.BytesReference;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;
import org.density.search.SearchHit;
import org.density.search.SearchHits;
import org.density.search.internal.InternalSearchResponse;
import org.density.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Predicate;

import static org.density.test.hamcrest.DensityAssertions.assertToXContentEquivalent;

public class SearchTemplateResponseTests extends AbstractXContentTestCase<SearchTemplateResponse> {

    @Override
    protected SearchTemplateResponse createTestInstance() {
        SearchTemplateResponse response = new SearchTemplateResponse();
        if (randomBoolean()) {
            response.setResponse(createSearchResponse());
        } else {
            response.setSource(createSource());
        }
        return response;
    }

    @Override
    protected SearchTemplateResponse doParseInstance(XContentParser parser) throws IOException {
        return SearchTemplateResponse.fromXContent(parser);
    }

    /**
     * For simplicity we create a minimal response, as there is already a dedicated
     * test class for search response parsing and serialization.
     */
    private static SearchResponse createSearchResponse() {
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, totalShards);
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();

        return new SearchResponse(
            internalSearchResponse,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    private static BytesReference createSource() {
        try {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("match")
                .field(randomAlphaOfLength(5), randomAlphaOfLength(10))
                .endObject()
                .endObject()
                .endObject();
            return BytesReference.bytes(source);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        String templateOutputField = SearchTemplateResponse.TEMPLATE_OUTPUT_FIELD.getPreferredName();
        return field -> field.equals(templateOutputField) || field.startsWith(templateOutputField + ".");
    }

    /**
     * Note that we can't rely on normal equals and hashCode checks, since {@link SearchResponse} doesn't
     * currently implement equals and hashCode. Instead, we compare the template outputs for equality,
     * and perform some sanity checks on the search response instances.
     */
    @Override
    protected void assertEqualInstances(SearchTemplateResponse expectedInstance, SearchTemplateResponse newInstance) {
        assertNotSame(newInstance, expectedInstance);

        BytesReference expectedSource = expectedInstance.getSource();
        BytesReference newSource = newInstance.getSource();
        assertEquals(expectedSource == null, newSource == null);
        if (expectedSource != null) {
            try {
                assertToXContentEquivalent(expectedSource, newSource, MediaTypeRegistry.JSON);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        assertEquals(expectedInstance.hasResponse(), newInstance.hasResponse());
        if (expectedInstance.hasResponse()) {
            SearchResponse expectedResponse = expectedInstance.getResponse();
            SearchResponse newResponse = newInstance.getResponse();

            assertEquals(expectedResponse.getHits().getTotalHits().value(), newResponse.getHits().getTotalHits().value());
            assertEquals(expectedResponse.getHits().getMaxScore(), newResponse.getHits().getMaxScore(), 0.0001);
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testSourceToXContent() throws IOException {
        SearchTemplateResponse response = new SearchTemplateResponse();

        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("query")
            .startObject("terms")
            .field("status", new String[] { "pending", "published" })
            .endObject()
            .endObject()
            .endObject();
        response.setSource(BytesReference.bytes(source));

        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedResponse = MediaTypeRegistry.contentBuilder(contentType)
            .startObject()
            .startObject("template_output")
            .startObject("query")
            .startObject("terms")
            .field("status", new String[] { "pending", "published" })
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        XContentBuilder actualResponse = MediaTypeRegistry.contentBuilder(contentType);
        response.toXContent(actualResponse, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedResponse), BytesReference.bytes(actualResponse), contentType);
    }

    public void testSearchResponseToXContent() throws IOException {
        SearchHit hit = new SearchHit(1, "id", Collections.emptyMap(), Collections.emptyMap());
        hit.score(2.0f);
        SearchHit[] hits = new SearchHit[] { hit };

        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
            new SearchHits(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f),
            null,
            null,
            null,
            false,
            null,
            1
        );
        SearchResponse searchResponse = new SearchResponse(
            internalSearchResponse,
            null,
            0,
            0,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        SearchTemplateResponse response = new SearchTemplateResponse();
        response.setResponse(searchResponse);

        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedResponse = MediaTypeRegistry.contentBuilder(contentType)
            .startObject()
            .field("took", 0)
            .field("timed_out", false)
            .startObject("_shards")
            .field("total", 0)
            .field("successful", 0)
            .field("skipped", 0)
            .field("failed", 0)
            .endObject()
            .startObject("hits")
            .startObject("total")
            .field("value", 100)
            .field("relation", "eq")
            .endObject()
            .field("max_score", 1.5F)
            .startArray("hits")
            .startObject()
            .field("_id", "id")
            .field("_score", 2.0F)
            .endObject()
            .endArray()
            .endObject()
            .field("status", 200)
            .endObject();

        XContentBuilder actualResponse = MediaTypeRegistry.contentBuilder(contentType);
        response.toXContent(actualResponse, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedResponse), BytesReference.bytes(actualResponse), contentType);
    }
}
