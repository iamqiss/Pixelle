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

package org.density.search.aggregations;

import org.density.common.SetOnce;
import org.density.common.settings.Settings;
import org.density.common.xcontent.XContentType;
import org.density.common.xcontent.json.JsonXContent;
import org.density.core.action.ActionListener;
import org.density.core.common.ParsingException;
import org.density.core.common.Strings;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;
import org.density.env.Environment;
import org.density.index.query.BaseQueryRewriteContext;
import org.density.index.query.MatchAllQueryBuilder;
import org.density.index.query.QueryBuilder;
import org.density.index.query.QueryRewriteContext;
import org.density.index.query.TermsQueryBuilder;
import org.density.index.query.WrapperQueryBuilder;
import org.density.script.Script;
import org.density.search.SearchModule;
import org.density.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.density.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.density.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.density.search.aggregations.pipeline.PipelineAggregator;
import org.density.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.density.test.AbstractQueryTestCase;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AggregatorFactoriesTests extends DensityTestCase {
    private NamedXContentRegistry xContentRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put("node.name", AbstractQueryTestCase.class.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();
        xContentRegistry = new NamedXContentRegistry(new SearchModule(settings, emptyList()).getNamedXContents());
    }

    public void testGetAggregatorFactories_returnsUnmodifiableList() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(AggregationBuilders.avg("foo"));
        Collection<AggregationBuilder> aggregatorFactories = builder.getAggregatorFactories();
        assertThat(aggregatorFactories.size(), equalTo(1));
        expectThrows(UnsupportedOperationException.class, () -> aggregatorFactories.add(AggregationBuilders.avg("bar")));
    }

    public void testGetPipelineAggregatorFactories_returnsUnmodifiableList() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addPipelineAggregator(
            PipelineAggregatorBuilders.avgBucket("foo", "path1")
        );
        Collection<PipelineAggregationBuilder> pipelineAggregatorFactories = builder.getPipelineAggregatorFactories();
        assertThat(pipelineAggregatorFactories.size(), equalTo(1));
        expectThrows(
            UnsupportedOperationException.class,
            () -> pipelineAggregatorFactories.add(PipelineAggregatorBuilders.avgBucket("bar", "path2"))
        );
    }

    public void testTwoTypes() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("in_stock")
            .startObject("filter")
            .startObject("range")
            .startObject("stock")
            .field("gt", 0)
            .endObject()
            .endObject()
            .endObject()
            .startObject("terms")
            .field("field", "stock")
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Found two aggregation type definitions in [in_stock]: [filter] and [terms]"));
    }

    public void testInvalidAggregationName() throws Exception {
        Matcher matcher = Pattern.compile("[^\\[\\]>]+").matcher("");
        String name;
        Random rand = random();
        int len = randomIntBetween(1, 5);
        char[] word = new char[len];
        while (true) {
            for (int i = 0; i < word.length; i++) {
                word[i] = (char) rand.nextInt(127);
            }
            name = String.valueOf(word);
            if (!matcher.reset(name).matches()) {
                break;
            }
        }

        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject(name)
            .startObject("filter")
            .startObject("range")
            .startObject("stock")
            .field("gt", 0)
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Invalid aggregation name [" + name + "]"));
    }

    public void testMissingName() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("by_date")
            .startObject("date_histogram")
            .field("field", "timestamp")
            .field("calendar_interval", "month")
            .endObject()
            .startObject("aggs")
            // the aggregation name is missing
            // .startObject("tag_count")
            .startObject("cardinality")
            .field("field", "tag")
            .endObject()
            // .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Expected [START_OBJECT] under [field], but got a [VALUE_STRING] in [cardinality]"));
    }

    public void testMissingType() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("by_date")
            .startObject("date_histogram")
            .field("field", "timestamp")
            .field("calendar_interval", "month")
            .endObject()
            .startObject("aggs")
            .startObject("tag_count")
            // the aggregation type is missing
            // .startObject("cardinality")
            .field("field", "tag")
            // .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Expected [START_OBJECT] under [field], but got a [VALUE_STRING] in [tag_count]"));
    }

    public void testInvalidType() throws Exception {
        XContentBuilder source = JsonXContent.contentBuilder()
            .startObject()
            .startObject("by_date")
            .startObject("date_histogram")
            .field("field", "timestamp")
            .field("calendar_interval", "month")
            .endObject()
            .startObject("aggs")
            .startObject("tags")
            // the aggregation type is invalid
            .startObject("term")
            .field("field", "tag")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(source);
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        Exception e = expectThrows(ParsingException.class, () -> AggregatorFactories.parseAggregators(parser));
        assertThat(e.toString(), containsString("Unknown aggregation type [term] did you mean [terms]?"));
    }

    public void testRewriteAggregation() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytesReference;
        try (XContentBuilder builder = MediaTypeRegistry.contentBuilder(xContentType)) {
            builder.startObject();
            {
                builder.startObject("terms");
                {
                    builder.array("title", "foo");
                }
                builder.endObject();
            }
            builder.endObject();
            bytesReference = BytesReference.bytes(builder);
        }
        FilterAggregationBuilder filterAggBuilder = new FilterAggregationBuilder("titles", new WrapperQueryBuilder(bytesReference));
        BucketScriptPipelineAggregationBuilder pipelineAgg = new BucketScriptPipelineAggregationBuilder("const", new Script("1"));
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(filterAggBuilder)
            .addPipelineAggregator(pipelineAgg);
        AggregatorFactories.Builder rewritten = builder.rewrite(new BaseQueryRewriteContext(xContentRegistry, null, null, () -> 0L));
        assertNotSame(builder, rewritten);
        Collection<AggregationBuilder> aggregatorFactories = rewritten.getAggregatorFactories();
        assertEquals(1, aggregatorFactories.size());
        assertThat(aggregatorFactories.iterator().next(), instanceOf(FilterAggregationBuilder.class));
        FilterAggregationBuilder rewrittenFilterAggBuilder = (FilterAggregationBuilder) aggregatorFactories.iterator().next();
        assertNotSame(filterAggBuilder, rewrittenFilterAggBuilder);
        assertNotEquals(filterAggBuilder, rewrittenFilterAggBuilder);
        // Check the filter was rewritten from a wrapper query to a terms query
        QueryBuilder rewrittenFilter = rewrittenFilterAggBuilder.getFilter();
        assertThat(rewrittenFilter, instanceOf(TermsQueryBuilder.class));

        // Check that a further rewrite returns the same aggregation factories builder
        AggregatorFactories.Builder secondRewritten = rewritten.rewrite(
            new BaseQueryRewriteContext(xContentRegistry, null, null, () -> 0L)
        );
        assertSame(rewritten, secondRewritten);
    }

    public void testRewritePipelineAggregationUnderAggregation() throws Exception {
        FilterAggregationBuilder filterAggBuilder = new FilterAggregationBuilder("titles", new MatchAllQueryBuilder()).subAggregation(
            new RewrittenPipelineAggregationBuilder()
        );
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(filterAggBuilder);
        QueryRewriteContext context = new BaseQueryRewriteContext(xContentRegistry, null, null, () -> 0L);
        AggregatorFactories.Builder rewritten = builder.rewrite(context);
        CountDownLatch latch = new CountDownLatch(1);
        context.executeAsyncActions(new ActionListener<Object>() {
            @Override
            public void onResponse(Object response) {
                assertNotSame(builder, rewritten);
                Collection<AggregationBuilder> aggregatorFactories = rewritten.getAggregatorFactories();
                assertEquals(1, aggregatorFactories.size());
                FilterAggregationBuilder rewrittenFilterAggBuilder = (FilterAggregationBuilder) aggregatorFactories.iterator().next();
                PipelineAggregationBuilder rewrittenPipeline = rewrittenFilterAggBuilder.getPipelineAggregations().iterator().next();
                assertThat(((RewrittenPipelineAggregationBuilder) rewrittenPipeline).setOnRewrite.get(), equalTo("rewritten"));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        latch.await();
    }

    public void testRewriteAggregationAtTopLevel() throws Exception {
        FilterAggregationBuilder filterAggBuilder = new FilterAggregationBuilder("titles", new MatchAllQueryBuilder());
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder().addAggregator(filterAggBuilder)
            .addPipelineAggregator(new RewrittenPipelineAggregationBuilder());
        QueryRewriteContext context = new BaseQueryRewriteContext(xContentRegistry, null, null, () -> 0L);
        AggregatorFactories.Builder rewritten = builder.rewrite(context);
        CountDownLatch latch = new CountDownLatch(1);
        context.executeAsyncActions(new ActionListener<Object>() {
            @Override
            public void onResponse(Object response) {
                assertNotSame(builder, rewritten);
                PipelineAggregationBuilder rewrittenPipeline = rewritten.getPipelineAggregatorFactories().iterator().next();
                assertThat(((RewrittenPipelineAggregationBuilder) rewrittenPipeline).setOnRewrite.get(), equalTo("rewritten"));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });
        latch.await();
    }

    public void testBuildPipelineTreeResolvesPipelineOrder() {
        AggregatorFactories.Builder builder = new AggregatorFactories.Builder();
        builder.addPipelineAggregator(PipelineAggregatorBuilders.avgBucket("bar", "foo"));
        builder.addPipelineAggregator(PipelineAggregatorBuilders.avgBucket("foo", "real"));
        builder.addAggregator(AggregationBuilders.avg("real").field("target"));
        PipelineTree tree = builder.buildPipelineTree();
        assertThat(tree.aggregators().stream().map(PipelineAggregator::name).collect(toList()), equalTo(Arrays.asList("foo", "bar")));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private class RewrittenPipelineAggregationBuilder extends AbstractPipelineAggregationBuilder<RewrittenPipelineAggregationBuilder> {
        private final Supplier<String> setOnRewrite;

        RewrittenPipelineAggregationBuilder() {
            super("test", "rewritten", Strings.EMPTY_ARRAY);
            setOnRewrite = null;
        }

        RewrittenPipelineAggregationBuilder(Supplier<String> setOnRewrite) {
            super("test", "rewritten", Strings.EMPTY_ARRAY);
            this.setOnRewrite = setOnRewrite;
        }

        @Override
        public PipelineAggregationBuilder rewrite(QueryRewriteContext context) throws IOException {
            if (setOnRewrite != null) {
                return this;
            }
            SetOnce<String> loaded = new SetOnce<>();
            context.registerAsyncAction((client, listener) -> {
                loaded.set("rewritten");
                listener.onResponse(null);
            });
            return new RewrittenPipelineAggregationBuilder(loaded::get);
        }

        @Override
        public String getWriteableName() {
            return "rewritten";
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected PipelineAggregator createInternal(Map<String, Object> metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void validate(ValidationContext context) {
            throw new UnsupportedOperationException();
        }
    }
}
