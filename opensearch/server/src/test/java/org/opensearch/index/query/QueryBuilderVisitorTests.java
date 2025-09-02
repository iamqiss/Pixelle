/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.query;

import org.apache.lucene.search.BooleanClause;
import org.density.test.AbstractBuilderTestCase;

import java.util.ArrayList;
import java.util.List;

public class QueryBuilderVisitorTests extends AbstractBuilderTestCase {

    public void testNoOpsVisitor() {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        List<QueryBuilder> visitedQueries = new ArrayList<>();
        QueryBuilderVisitor qbv = createTestVisitor(visitedQueries);
        boolQueryBuilder.visit(qbv);
        QueryBuilderVisitor subQbv = qbv.getChildVisitor(BooleanClause.Occur.MUST_NOT);
        assertEquals(0, visitedQueries.size());
        assertEquals(qbv, subQbv);
    }

    protected static QueryBuilderVisitor createTestVisitor(List<QueryBuilder> visitedQueries) {
        return QueryBuilderVisitor.NO_OP_VISITOR;
    }
}
