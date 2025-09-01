/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search.query;

import org.density.index.query.QueryBuilder;
import org.density.index.query.TermQueryBuilder;
import org.density.protobufs.FieldValue;
import org.density.protobufs.QueryContainer;
import org.density.protobufs.TermQuery;
import org.density.test.DensityTestCase;

public class TermQueryBuilderProtoConverterTests extends DensityTestCase {

    private TermQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new TermQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals("Converter should handle TERM case", QueryContainer.QueryContainerCase.TERM, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        // Create a QueryContainer with TermQuery
        FieldValue fieldValue = FieldValue.newBuilder().setStringValue("test-value").build();
        TermQuery termQuery = TermQuery.newBuilder()
            .setField("test-field")
            .setValue(fieldValue)
            .setBoost(2.0f)
            .setUnderscoreName("test_query")
            .setCaseInsensitive(true)
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setTerm(termQuery).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermQueryBuilder", queryBuilder instanceof TermQueryBuilder);
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", termQueryBuilder.fieldName());
        assertEquals("Value should match", "test-value", termQueryBuilder.value());
        assertEquals("Boost should match", 2.0f, termQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termQueryBuilder.queryName());
        assertTrue("Case insensitive should be true", termQueryBuilder.caseInsensitive());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Term query'",
            exception.getMessage().contains("does not contain a Term query")
        );
    }
}
