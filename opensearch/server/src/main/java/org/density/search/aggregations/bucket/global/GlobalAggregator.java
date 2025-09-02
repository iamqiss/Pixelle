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

package org.density.search.aggregations.bucket.global;

import org.apache.lucene.index.LeafReaderContext;
import org.density.search.aggregations.AggregatorFactories;
import org.density.search.aggregations.CardinalityUpperBound;
import org.density.search.aggregations.InternalAggregation;
import org.density.search.aggregations.LeafBucketCollector;
import org.density.search.aggregations.LeafBucketCollectorBase;
import org.density.search.aggregations.bucket.BucketsAggregator;
import org.density.search.aggregations.bucket.SingleBucketAggregator;
import org.density.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregate all docs from the global search scope.
 *
 * @density.internal
 */
public class GlobalAggregator extends BucketsAggregator implements SingleBucketAggregator {

    public GlobalAggregator(String name, AggregatorFactories subFactories, SearchContext aggregationContext, Map<String, Object> metadata)
        throws IOException {
        super(name, subFactories, aggregationContext, null, CardinalityUpperBound.ONE, metadata);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        return new LeafBucketCollectorBase(sub, null) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0 : "global aggregator can only be a top level aggregator";
                collectBucket(sub, doc, bucket);
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        assert owningBucketOrds.length == 1 && owningBucketOrds[0] == 0 : "global aggregator can only be a top level aggregator";
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalGlobal(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException(
            "global aggregations cannot serve as sub-aggregations, hence should never be called on #buildEmptyAggregations"
        );
    }
}
