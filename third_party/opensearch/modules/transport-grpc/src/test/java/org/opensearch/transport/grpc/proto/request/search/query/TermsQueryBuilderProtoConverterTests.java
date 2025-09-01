/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search.query;

import org.density.index.query.QueryBuilder;
import org.density.index.query.TermsQueryBuilder;
import org.density.protobufs.QueryContainer;
import org.density.protobufs.StringArray;
import org.density.protobufs.TermsLookupFieldStringArrayMap;
import org.density.protobufs.TermsQueryField;
import org.density.protobufs.ValueType;
import org.density.test.DensityTestCase;

import java.util.HashMap;
import java.util.Map;

public class TermsQueryBuilderProtoConverterTests extends DensityTestCase {

    private TermsQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new TermsQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        // Test that the converter returns the correct QueryContainerCase
        assertEquals("Converter should handle TERMS case", QueryContainer.QueryContainerCase.TERMS, converter.getHandledQueryCase());
    }

    public void testFromProto() {
        // Create a QueryContainer with TermsQuery
        StringArray stringArray = StringArray.newBuilder().addStringArray("value1").addStringArray("value2").build();
        TermsLookupFieldStringArrayMap termsLookupFieldStringArrayMap = TermsLookupFieldStringArrayMap.newBuilder()
            .setStringArray(stringArray)
            .build();
        Map<String, TermsLookupFieldStringArrayMap> termsLookupFieldStringArrayMapMap = new HashMap<>();
        termsLookupFieldStringArrayMapMap.put("test-field", termsLookupFieldStringArrayMap);
        TermsQueryField termsQueryField = TermsQueryField.newBuilder()
            .putAllTermsLookupFieldStringArrayMap(termsLookupFieldStringArrayMapMap)
            .setBoost(2.0f)
            .setUnderscoreName("test_query")
            .setValueType(ValueType.VALUE_TYPE_DEFAULT)
            .build();
        QueryContainer queryContainer = QueryContainer.newBuilder().setTerms(termsQueryField).build();

        // Convert the query
        QueryBuilder queryBuilder = converter.fromProto(queryContainer);

        // Verify the result
        assertNotNull("QueryBuilder should not be null", queryBuilder);
        assertTrue("QueryBuilder should be a TermsQueryBuilder", queryBuilder instanceof TermsQueryBuilder);
        TermsQueryBuilder termsQueryBuilder = (TermsQueryBuilder) queryBuilder;
        assertEquals("Field name should match", "test-field", termsQueryBuilder.fieldName());
        assertEquals("Values size should match", 2, termsQueryBuilder.values().size());
        assertEquals("First value should match", "value1", termsQueryBuilder.values().get(0));
        assertEquals("Second value should match", "value2", termsQueryBuilder.values().get(1));
        assertEquals("Boost should match", 2.0f, termsQueryBuilder.boost(), 0.0f);
        assertEquals("Query name should match", "test_query", termsQueryBuilder.queryName());
        assertEquals("Value type should match", TermsQueryBuilder.ValueType.DEFAULT, termsQueryBuilder.valueType());
    }

    public void testFromProtoWithInvalidContainer() {
        // Create a QueryContainer with a different query type
        QueryContainer emptyContainer = QueryContainer.newBuilder().build();

        // Test that the converter throws an exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(emptyContainer));

        // Verify the exception message
        assertTrue(
            "Exception message should mention 'does not contain a Terms query'",
            exception.getMessage().contains("does not contain a Terms query")
        );
    }
}
