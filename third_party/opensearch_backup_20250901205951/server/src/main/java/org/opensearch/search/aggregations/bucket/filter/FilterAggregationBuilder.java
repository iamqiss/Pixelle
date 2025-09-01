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

package org.density.search.aggregations.bucket.filter;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;
import org.density.index.query.QueryBuilder;
import org.density.index.query.QueryRewriteContext;
import org.density.index.query.QueryShardContext;
import org.density.index.query.Rewriteable;
import org.density.search.aggregations.AbstractAggregationBuilder;
import org.density.search.aggregations.AggregationBuilder;
import org.density.search.aggregations.AggregatorFactories;
import org.density.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.density.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;

/**
 * Aggregation Builder for filter agg
 *
 * @density.internal
 */
public class FilterAggregationBuilder extends AbstractAggregationBuilder<FilterAggregationBuilder> {
    public static final String NAME = "filter";

    private final QueryBuilder filter;

    /**
     * @param name
     *            the name of this aggregation
     * @param filter
     *            Set the filter to use, only documents that match this
     *            filter will fall into the bucket defined by this
     *            {@link Filter} aggregation.
     */
    public FilterAggregationBuilder(String name, QueryBuilder filter) {
        super(name);
        if (filter == null) {
            throw new IllegalArgumentException("[filter] must not be null: [" + name + "]");
        }
        this.filter = filter;
    }

    protected FilterAggregationBuilder(
        FilterAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.filter = clone.filter;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new FilterAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public FilterAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        filter = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(filter);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregationBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        QueryBuilder result = Rewriteable.rewrite(filter, queryShardContext);
        if (result != filter) {
            return new FilterAggregationBuilder(getName(), result);
        }
        return this;
    }

    @Override
    protected AggregatorFactory doBuild(
        QueryShardContext queryShardContext,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new FilterAggregatorFactory(name, filter, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (filter != null) {
            filter.toXContent(builder, params);
        }
        return builder;
    }

    public static FilterAggregationBuilder parse(XContentParser parser, String aggregationName) throws IOException {
        QueryBuilder filter = parseInnerQueryBuilder(parser);
        return new FilterAggregationBuilder(aggregationName, filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        FilterAggregationBuilder other = (FilterAggregationBuilder) obj;
        return Objects.equals(filter, other.filter);
    }

    @Override
    public String getType() {
        return NAME;
    }

    public QueryBuilder getFilter() {
        return filter;
    }
}
