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

package org.density.search.basic;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.tests.util.English;
import org.density.DensityException;
import org.density.action.DocWriteResponse;
import org.density.action.admin.indices.refresh.RefreshResponse;
import org.density.action.index.IndexResponse;
import org.density.action.search.SearchPhaseExecutionException;
import org.density.action.search.SearchResponse;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.settings.Settings;
import org.density.common.settings.Settings.Builder;
import org.density.common.unit.TimeValue;
import org.density.common.xcontent.XContentFactory;
import org.density.index.MockEngineFactoryPlugin;
import org.density.index.query.QueryBuilders;
import org.density.plugins.Plugin;
import org.density.search.sort.SortOrder;
import org.density.test.DensityIntegTestCase;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;
import org.density.test.engine.MockEngineSupport;
import org.density.test.engine.ThrowingLeafReaderWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;

public class SearchWithRandomExceptionsIT extends ParameterizedStaticSettingsDensityIntegTestCase {

    public SearchWithRandomExceptionsIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(RandomExceptionDirectoryReaderWrapper.TestPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testRandomExceptions() throws IOException, InterruptedException, ExecutionException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("test")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        final double lowLevelRate;
        final double topLevelRate;
        if (frequently()) {
            if (randomBoolean()) {
                if (randomBoolean()) {
                    lowLevelRate = 1.0 / between(2, 10);
                    topLevelRate = 0.0d;
                } else {
                    topLevelRate = 1.0 / between(2, 10);
                    lowLevelRate = 0.0d;
                }
            } else {
                lowLevelRate = 1.0 / between(2, 10);
                topLevelRate = 1.0 / between(2, 10);
            }
        } else {
            // rarely no exception
            topLevelRate = 0d;
            lowLevelRate = 0d;
        }

        Builder settings = Settings.builder()
            .put(indexSettings())
            .put(EXCEPTION_TOP_LEVEL_RATIO_KEY, topLevelRate)
            .put(EXCEPTION_LOW_LEVEL_RATIO_KEY, lowLevelRate)
            .put(MockEngineSupport.WRAP_READER_RATIO.getKey(), 1.0d);
        logger.info("creating index: [test] using settings: [{}]", settings.build());
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(mapping));
        ensureSearchable();
        final int numDocs = between(10, 100);
        int numCreated = 0;
        boolean[] added = new boolean[numDocs];
        for (int i = 0; i < numDocs; i++) {
            try {
                IndexResponse indexResponse = client().prepareIndex("test")
                    .setId("" + i)
                    .setTimeout(TimeValue.timeValueSeconds(1))
                    .setSource("test", English.intToEnglish(i))
                    .get();
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    numCreated++;
                    added[i] = true;
                }
            } catch (DensityException ex) {}
        }
        logger.info("Start Refresh");
        // don't assert on failures here
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().get();
        final boolean refreshFailed = refreshResponse.getShardFailures().length != 0 || refreshResponse.getFailedShards() != 0;
        logger.info(
            "Refresh failed [{}] numShardsFailed: [{}], shardFailuresLength: [{}], successfulShards: [{}], totalShards: [{}] ",
            refreshFailed,
            refreshResponse.getFailedShards(),
            refreshResponse.getShardFailures().length,
            refreshResponse.getSuccessfulShards(),
            refreshResponse.getTotalShards()
        );

        NumShards test = getNumShards("test");
        final int numSearches = scaledRandomIntBetween(100, 200);
        // we don't check anything here really just making sure we don't leave any open files or a broken index behind.
        for (int i = 0; i < numSearches; i++) {
            try {
                int docToQuery = between(0, numDocs - 1);
                int expectedResults = added[docToQuery] ? 1 : 0;
                logger.info("Searching for [test:{}]", English.intToEnglish(docToQuery));
                SearchResponse searchResponse = client().prepareSearch()
                    .setQuery(QueryBuilders.matchQuery("test", English.intToEnglish(docToQuery)))
                    .setSize(expectedResults)
                    .get();
                logger.info("Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(), test.numPrimaries);
                if (searchResponse.getSuccessfulShards() == test.numPrimaries && !refreshFailed) {
                    assertResultsAndLogOnFailure(expectedResults, searchResponse);
                }
                // check match all
                searchResponse = client().prepareSearch()
                    .setQuery(QueryBuilders.matchAllQuery())
                    .setSize(numCreated)
                    .addSort("_id", SortOrder.ASC)
                    .get();
                logger.info("Match all Successful shards: [{}]  numShards: [{}]", searchResponse.getSuccessfulShards(), test.numPrimaries);
                if (searchResponse.getSuccessfulShards() == test.numPrimaries && !refreshFailed) {
                    assertResultsAndLogOnFailure(numCreated, searchResponse);
                }

            } catch (SearchPhaseExecutionException ex) {
                logger.info("expected SearchPhaseException: [{}]", ex.getMessage());
            }
        }

        // as the index refresh may fail, so the translog in the index will be not flushed,
        // and `TranslogWriter.buffer` is not null, which causes arrays not been released,
        // so we need to close the index to release the arrays.
        cluster().wipeIndices("test");
    }

    public static final String EXCEPTION_TOP_LEVEL_RATIO_KEY = "index.engine.exception.ratio.top";
    public static final String EXCEPTION_LOW_LEVEL_RATIO_KEY = "index.engine.exception.ratio.low";

    public static class RandomExceptionDirectoryReaderWrapper extends MockEngineSupport.DirectoryReaderWrapper {

        public static class TestPlugin extends MockEngineFactoryPlugin {
            public static final Setting<Double> EXCEPTION_TOP_LEVEL_RATIO_SETTING = Setting.doubleSetting(
                EXCEPTION_TOP_LEVEL_RATIO_KEY,
                0.1d,
                0.0d,
                Property.IndexScope
            );
            public static final Setting<Double> EXCEPTION_LOW_LEVEL_RATIO_SETTING = Setting.doubleSetting(
                EXCEPTION_LOW_LEVEL_RATIO_KEY,
                0.1d,
                0.0d,
                Property.IndexScope
            );

            @Override
            public List<Setting<?>> getSettings() {
                List<Setting<?>> settings = new ArrayList<>();
                settings.addAll(super.getSettings());
                settings.add(EXCEPTION_TOP_LEVEL_RATIO_SETTING);
                settings.add(EXCEPTION_LOW_LEVEL_RATIO_SETTING);
                return settings;
            }

            @Override
            protected Class<? extends FilterDirectoryReader> getReaderWrapperClass() {
                return RandomExceptionDirectoryReaderWrapper.class;
            }
        }

        private final Settings settings;

        static class ThrowingSubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper implements ThrowingLeafReaderWrapper.Thrower {
            private final Random random;
            private final double topLevelRatio;
            private final double lowLevelRatio;

            ThrowingSubReaderWrapper(Settings settings) {
                final long seed = DensityIntegTestCase.INDEX_TEST_SEED_SETTING.get(settings);
                this.topLevelRatio = settings.getAsDouble(EXCEPTION_TOP_LEVEL_RATIO_KEY, 0.1d);
                this.lowLevelRatio = settings.getAsDouble(EXCEPTION_LOW_LEVEL_RATIO_KEY, 0.1d);
                this.random = new Random(seed);
            }

            @Override
            public LeafReader wrap(LeafReader reader) {
                return new ThrowingLeafReaderWrapper(reader, this);
            }

            @Override
            public void maybeThrow(ThrowingLeafReaderWrapper.Flags flag) throws IOException {
                switch (flag) {
                    case Fields:
                    case TermVectors:
                    case Terms:
                    case TermsEnum:
                    case Intersect:
                    case Norms:
                    case NumericDocValues:
                    case BinaryDocValues:
                    case SortedDocValues:
                    case SortedSetDocValues:
                        if (random.nextDouble() < topLevelRatio) {
                            throw new IOException("Forced top level Exception on [" + flag.name() + "]");
                        }
                        break;
                    case DocsEnum:
                    case DocsAndPositionsEnum:
                        if (random.nextDouble() < lowLevelRatio) {
                            throw new IOException("Forced low level Exception on [" + flag.name() + "]");
                        }
                        break;
                }
            }

            @Override
            public boolean wrapTerms(String field) {
                return true;
            }
        }

        public RandomExceptionDirectoryReaderWrapper(DirectoryReader in, Settings settings) throws IOException {
            super(in, new ThrowingSubReaderWrapper(settings));
            this.settings = settings;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new RandomExceptionDirectoryReaderWrapper(in, settings);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

}
