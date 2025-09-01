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

package org.density.search.functionscore;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.density.action.index.IndexRequestBuilder;
import org.density.action.search.SearchResponse;
import org.density.action.search.SearchType;
import org.density.common.lucene.search.function.CombineFunction;
import org.density.common.lucene.search.function.Functions;
import org.density.common.settings.Settings;
import org.density.index.fielddata.ScriptDocValues;
import org.density.plugins.Plugin;
import org.density.plugins.ScriptPlugin;
import org.density.script.ExplainableScoreScript;
import org.density.script.ScoreScript;
import org.density.script.Script;
import org.density.script.ScriptContext;
import org.density.script.ScriptEngine;
import org.density.script.ScriptType;
import org.density.search.SearchHit;
import org.density.search.SearchHits;
import org.density.search.lookup.SearchLookup;
import org.density.test.DensityIntegTestCase.ClusterScope;
import org.density.test.DensityIntegTestCase.Scope;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;
import org.density.test.hamcrest.DensityAssertions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.density.common.xcontent.XContentFactory.jsonBuilder;
import static org.density.index.query.QueryBuilders.functionScoreQuery;
import static org.density.index.query.QueryBuilders.termQuery;
import static org.density.index.query.functionscore.ScoreFunctionBuilders.scriptFunction;
import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.search.builder.SearchSourceBuilder.searchSource;
import static org.density.transport.client.Requests.searchRequest;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class ExplainableScriptIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    public ExplainableScriptIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public static class ExplainableScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
            return new ScriptEngine() {
                @Override
                public String getType() {
                    return "test";
                }

                @Override
                public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
                    assert scriptSource.equals("explainable_script");
                    assert context == ScoreScript.CONTEXT;
                    ScoreScript.Factory factory = (params1, lookup, indexSearcher) -> new ScoreScript.LeafFactory() {
                        @Override
                        public boolean needs_score() {
                            return false;
                        }

                        @Override
                        public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
                            return new MyScript(params1, lookup, indexSearcher, ctx);
                        }
                    };
                    return context.factoryClazz.cast(factory);
                }

                @Override
                public Set<ScriptContext<?>> getSupportedContexts() {
                    return Collections.singleton(ScoreScript.CONTEXT);
                }
            };
        }
    }

    static class MyScript extends ScoreScript implements ExplainableScoreScript {

        MyScript(Map<String, Object> params, SearchLookup lookup, IndexSearcher indexSearcher, LeafReaderContext leafContext) {
            super(params, lookup, indexSearcher, leafContext);
        }

        @Override
        public Explanation explain(Explanation subQueryScore) throws IOException {
            return explain(subQueryScore, null);
        }

        @Override
        public Explanation explain(Explanation subQueryScore, String functionName) throws IOException {
            Explanation scoreExp = Explanation.match(subQueryScore.getValue(), "_score: ", subQueryScore);
            return Explanation.match(
                (float) (execute(null)),
                "This script" + Functions.nameOrEmptyFunc(functionName) + " returned " + execute(null),
                scoreExp
            );
        }

        @Override
        public double execute(ExplanationHolder explanation) {
            return ((Number) ((ScriptDocValues) getDoc().get("number_field")).get(0)).doubleValue();
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ExplainableScriptPlugin.class);
    }

    public void testExplainScript() throws InterruptedException, IOException, ExecutionException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            indexRequests.add(
                client().prepareIndex("test")
                    .setId(Integer.toString(i))
                    .setSource(jsonBuilder().startObject().field("number_field", i).field("text", "text").endObject())
            );
        }
        indexRandom(true, true, indexRequests);
        client().admin().indices().prepareRefresh().get();
        ensureYellow();
        SearchResponse response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().explain(true)
                        .query(
                            functionScoreQuery(
                                termQuery("text", "text"),
                                scriptFunction(new Script(ScriptType.INLINE, "test", "explainable_script", Collections.emptyMap()))
                            ).boostMode(CombineFunction.REPLACE)
                        )
                )
        ).actionGet();

        DensityAssertions.assertNoFailures(response);
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits().value(), equalTo(20L));
        int idCounter = 19;
        for (SearchHit hit : hits.getHits()) {
            assertThat(hit.getId(), equalTo(Integer.toString(idCounter)));
            assertThat(hit.getExplanation().toString(), containsString(Double.toString(idCounter)));

            // Since Apache Lucene 9.8, the scores are not computed because script (see please ExplainableScriptPlugin)
            // says "needs_score() == false"
            // 19.0 = min of:
            // 19.0 = This script returned 19.0
            // 0.0 = _score:
            // 0.0 = weight(text:text in 0) [PerFieldSimilarity], result of:
            // 0.0 = score(freq=1.0), with freq of:
            // 1.0 = freq, occurrences of term within document
            // 3.4028235E38 = maxBoost

            assertThat(hit.getExplanation().toString(), containsString("1.0 = freq, occurrences of term within document"));
            assertThat(hit.getExplanation().getDetails().length, equalTo(2));
            idCounter--;
        }
    }

    public void testExplainScriptWithName() throws InterruptedException, IOException, ExecutionException {
        List<IndexRequestBuilder> indexRequests = new ArrayList<>();
        indexRequests.add(
            client().prepareIndex("test")
                .setId(Integer.toString(1))
                .setSource(jsonBuilder().startObject().field("number_field", 1).field("text", "text").endObject())
        );
        indexRandom(true, true, indexRequests);
        client().admin().indices().prepareRefresh().get();
        ensureYellow();
        SearchResponse response = client().search(
            searchRequest().searchType(SearchType.QUERY_THEN_FETCH)
                .source(
                    searchSource().explain(true)
                        .query(
                            functionScoreQuery(
                                termQuery("text", "text"),
                                scriptFunction(new Script(ScriptType.INLINE, "test", "explainable_script", Collections.emptyMap()), "func1")
                            ).boostMode(CombineFunction.REPLACE)
                        )
                )
        ).actionGet();

        DensityAssertions.assertNoFailures(response);
        SearchHits hits = response.getHits();
        assertThat(hits.getTotalHits().value(), equalTo(1L));
        assertThat(hits.getHits()[0].getId(), equalTo("1"));
        assertThat(hits.getHits()[0].getExplanation().getDetails(), arrayWithSize(2));
        assertThat(hits.getHits()[0].getExplanation().getDetails()[0].getDescription(), containsString("_name: func1"));
    }

}
