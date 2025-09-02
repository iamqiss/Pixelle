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

import org.apache.lucene.util.BytesRef;
import org.density.Version;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.settings.Settings;
import org.density.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.common.io.stream.StreamInput;
import org.density.search.DocValueFormat;
import org.density.search.SearchModule;
import org.density.search.aggregations.bucket.histogram.InternalDateHistogramTests;
import org.density.search.aggregations.bucket.terms.StringTerms;
import org.density.search.aggregations.bucket.terms.StringTermsTests;
import org.density.search.aggregations.bucket.terms.TermsAggregator;
import org.density.search.aggregations.pipeline.InternalSimpleValueTests;
import org.density.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.density.search.aggregations.pipeline.PipelineAggregator;
import org.density.test.InternalAggregationTestCase;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class InternalAggregationsTests extends DensityTestCase {

    private final NamedWriteableRegistry registry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables()
    );

    public void testReduceEmptyAggs() {
        List<InternalAggregations> aggs = Collections.emptyList();
        InternalAggregation.ReduceContextBuilder builder = InternalAggregationTestCase.emptyReduceContextBuilder();
        InternalAggregation.ReduceContext reduceContext = randomBoolean() ? builder.forFinalReduction() : builder.forPartialReduction();
        assertNull(InternalAggregations.reduce(aggs, reduceContext));
    }

    public void testNonFinalReduceTopLevelPipelineAggs() {
        InternalAggregation terms = new StringTerms(
            "name",
            BucketOrder.key(true),
            BucketOrder.key(true),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            10,
            Collections.emptyList(),
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, 25)
        );
        List<InternalAggregations> aggs = singletonList(InternalAggregations.from(Collections.singletonList(terms)));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(aggs, maxBucketReduceContext().forPartialReduction());
        assertEquals(1, reducedAggs.aggregations.size());
    }

    public void testFinalReduceTopLevelPipelineAggs() {
        InternalAggregation terms = new StringTerms(
            "name",
            BucketOrder.key(true),
            BucketOrder.key(true),
            Collections.emptyMap(),
            DocValueFormat.RAW,
            25,
            false,
            10,
            Collections.emptyList(),
            0,
            new TermsAggregator.BucketCountThresholds(1, 0, 10, 25)
        );

        InternalAggregations aggs = InternalAggregations.from(Collections.singletonList(terms));
        InternalAggregations reducedAggs = InternalAggregations.topLevelReduce(
            Collections.singletonList(aggs),
            maxBucketReduceContext().forFinalReduction()
        );
        assertEquals(2, reducedAggs.aggregations.size());
    }

    private InternalAggregation.ReduceContextBuilder maxBucketReduceContext() {
        MaxBucketPipelineAggregationBuilder maxBucketPipelineAggregationBuilder = new MaxBucketPipelineAggregationBuilder("test", "test");
        PipelineAggregator.PipelineTree tree = new PipelineAggregator.PipelineTree(
            emptyMap(),
            singletonList(maxBucketPipelineAggregationBuilder.create())
        );
        return InternalAggregationTestCase.emptyReduceContextBuilder(tree);
    }

    public static InternalAggregations createTestInstance() throws Exception {
        List<InternalAggregation> aggsList = new ArrayList<>();
        if (randomBoolean()) {
            StringTermsTests stringTermsTests = new StringTermsTests();
            stringTermsTests.init();
            stringTermsTests.setUp();
            aggsList.add(stringTermsTests.createTestInstance());
        }
        if (randomBoolean()) {
            InternalDateHistogramTests dateHistogramTests = new InternalDateHistogramTests();
            dateHistogramTests.setUp();
            aggsList.add(dateHistogramTests.createTestInstance());
        }
        if (randomBoolean()) {
            InternalSimpleValueTests simpleValueTests = new InternalSimpleValueTests();
            aggsList.add(simpleValueTests.createTestInstance());
        }
        return new InternalAggregations(aggsList);
    }

    public void testSerialization() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        writeToAndReadFrom(aggregations, Version.CURRENT, 0);
    }

    public void testSerializedSize() throws Exception {
        InternalAggregations aggregations = createTestInstance();
        assertThat(aggregations.getSerializedSize(), equalTo((long) serialize(aggregations, Version.CURRENT).length));
    }

    private void writeToAndReadFrom(InternalAggregations aggregations, Version version, int iteration) throws IOException {
        BytesRef serializedAggs = serialize(aggregations, version);
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(serializedAggs.bytes), registry)) {
            in.setVersion(version);
            InternalAggregations deserialized = InternalAggregations.readFrom(in);
            assertEquals(aggregations.aggregations, deserialized.aggregations);
            if (iteration < 2) {
                writeToAndReadFrom(deserialized, version, iteration + 1);
            }
        }
    }

    private BytesRef serialize(InternalAggregations aggs, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            aggs.writeTo(out);
            return out.bytes().toBytesRef();
        }
    }
}
