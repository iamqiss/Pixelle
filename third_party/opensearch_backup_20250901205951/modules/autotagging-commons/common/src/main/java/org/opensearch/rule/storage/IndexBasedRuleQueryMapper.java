/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.storage;

import org.density.common.annotation.ExperimentalApi;
import org.density.index.query.BoolQueryBuilder;
import org.density.index.query.QueryBuilder;
import org.density.index.query.QueryBuilders;
import org.density.rule.RuleQueryMapper;
import org.density.rule.action.GetRuleRequest;
import org.density.rule.autotagging.Attribute;

import java.util.Map;
import java.util.Set;

/**
 * This class is used to build density index based query object
 */
@ExperimentalApi
public class IndexBasedRuleQueryMapper implements RuleQueryMapper<QueryBuilder> {

    /**
     * Default constructor
     */
    public IndexBasedRuleQueryMapper() {}

    @Override
    public QueryBuilder from(GetRuleRequest request) {
        final BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        final Map<Attribute, Set<String>> attributeFilters = request.getAttributeFilters();
        final String id = request.getId();

        boolQuery.filter(QueryBuilders.existsQuery(request.getFeatureType().getName()));
        if (id != null) {
            return boolQuery.must(QueryBuilders.termQuery("_id", id));
        }
        for (Map.Entry<Attribute, Set<String>> entry : attributeFilters.entrySet()) {
            Attribute attribute = entry.getKey();
            Set<String> values = entry.getValue();
            if (values != null && !values.isEmpty()) {
                BoolQueryBuilder attributeQuery = QueryBuilders.boolQuery();
                for (String value : values) {
                    attributeQuery.should(QueryBuilders.matchQuery(attribute.getName(), value));
                }
                boolQuery.must(attributeQuery);
            }
        }
        return boolQuery;
    }

    @Override
    public QueryBuilder getCardinalityQuery() {
        return QueryBuilders.matchAllQuery();
    }
}
