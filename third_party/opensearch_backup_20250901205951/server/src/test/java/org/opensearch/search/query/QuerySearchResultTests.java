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

package org.density.search.query;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.density.Version;
import org.density.action.OriginalIndices;
import org.density.action.OriginalIndicesTests;
import org.density.action.search.SearchRequest;
import org.density.common.UUIDs;
import org.density.common.lucene.search.TopDocsAndMaxScore;
import org.density.common.settings.Settings;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.index.shard.ShardId;
import org.density.search.DocValueFormat;
import org.density.search.SearchModule;
import org.density.search.SearchShardTarget;
import org.density.search.aggregations.Aggregations;
import org.density.search.aggregations.InternalAggregationsTests;
import org.density.search.internal.AliasFilter;
import org.density.search.internal.ShardSearchContextId;
import org.density.search.internal.ShardSearchRequest;
import org.density.search.suggest.SuggestTests;
import org.density.test.DensityTestCase;

import static java.util.Collections.emptyList;

public class QuerySearchResultTests extends DensityTestCase {

    private final NamedWriteableRegistry namedWriteableRegistry;

    public QuerySearchResultTests() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        this.namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    private static QuerySearchResult createTestInstance() throws Exception {
        ShardId shardId = new ShardId("index", "uuid", randomInt());
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        ShardSearchRequest shardSearchRequest = new ShardSearchRequest(
            OriginalIndicesTests.randomOriginalIndices(),
            searchRequest,
            shardId,
            1,
            new AliasFilter(null, Strings.EMPTY_ARRAY),
            1.0f,
            randomNonNegativeLong(),
            null,
            new String[0]
        );
        QuerySearchResult result = new QuerySearchResult(
            new ShardSearchContextId(UUIDs.base64UUID(), randomLong()),
            new SearchShardTarget("node", shardId, null, OriginalIndices.NONE),
            shardSearchRequest
        );
        if (randomBoolean()) {
            result.terminatedEarly(randomBoolean());
        }
        TopDocs topDocs = new TopDocs(new TotalHits(randomLongBetween(0, Long.MAX_VALUE), TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
        result.topDocs(new TopDocsAndMaxScore(topDocs, randomBoolean() ? Float.NaN : randomFloat()), new DocValueFormat[0]);
        result.size(randomInt());
        result.from(randomInt());
        if (randomBoolean()) {
            result.suggest(SuggestTests.createTestItem());
        }
        if (randomBoolean()) {
            result.aggregations(InternalAggregationsTests.createTestInstance());
        }
        return result;
    }

    public void testSerialization() throws Exception {
        QuerySearchResult querySearchResult = createTestInstance();
        QuerySearchResult deserialized = copyWriteable(querySearchResult, namedWriteableRegistry, QuerySearchResult::new);
        assertEquals(querySearchResult.getContextId().getId(), deserialized.getContextId().getId());
        assertNull(deserialized.getSearchShardTarget());
        assertEquals(querySearchResult.topDocs().maxScore, deserialized.topDocs().maxScore, 0f);
        assertEquals(querySearchResult.topDocs().topDocs.totalHits, deserialized.topDocs().topDocs.totalHits);
        assertEquals(querySearchResult.from(), deserialized.from());
        assertEquals(querySearchResult.size(), deserialized.size());
        assertEquals(querySearchResult.hasAggs(), deserialized.hasAggs());
        if (deserialized.hasAggs()) {
            Aggregations aggs = querySearchResult.consumeAggs().expand();
            Aggregations deserializedAggs = deserialized.consumeAggs().expand();
            assertEquals(aggs.asList(), deserializedAggs.asList());
        }
        assertEquals(querySearchResult.terminatedEarly(), deserialized.terminatedEarly());
    }

    public void testNullResponse() throws Exception {
        QuerySearchResult querySearchResult = QuerySearchResult.nullInstance();
        QuerySearchResult deserialized = copyWriteable(querySearchResult, namedWriteableRegistry, QuerySearchResult::new, Version.CURRENT);
        assertEquals(querySearchResult.isNull(), deserialized.isNull());
    }
}
