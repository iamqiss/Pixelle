/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.query;

import org.density.search.internal.SearchContext;
import org.density.test.DensityTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Optional;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public class QueryCollectorContextSpecRegistryTests extends DensityTestCase {
    @Mock
    private QueryCollectorContextSpecFactory mockFactory1;

    @Mock
    private SearchContext mockSearchContext;

    @Mock
    private QueryCollectorContextSpec mockSpec;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        // Clear registry before each test
        QueryCollectorContextSpecRegistry.getCollectorContextSpecFactories().clear();
    }

    public void testRegisterFactory() {
        QueryCollectorContextSpecRegistry.registerFactory(mockFactory1);
        assertEquals(1, QueryCollectorContextSpecRegistry.getCollectorContextSpecFactories().size());
        assertTrue(QueryCollectorContextSpecRegistry.getCollectorContextSpecFactories().contains(mockFactory1));
    }

    public void testGetQueryCollectorContextSpec_WithValidSpec() throws IOException {

        QueryCollectorArguments mockArguments = new QueryCollectorArguments.Builder().build();
        // Given
        QueryCollectorContextSpecRegistry.registerFactory(mockFactory1);
        when(mockFactory1.createQueryCollectorContextSpec(mockSearchContext, mockSearchContext.query(), mockArguments)).thenReturn(
            Optional.of(mockSpec)
        );

        // When
        Optional<QueryCollectorContextSpec> result = QueryCollectorContextSpecRegistry.getQueryCollectorContextSpec(
            mockSearchContext,
            mockSearchContext.query(),
            mockArguments
        );

        // Then
        assertTrue(result.isPresent());
        assertEquals(mockSpec, result.get());
    }

    public void testGetQueryCollectorContextSpec_NoFactories() throws IOException {

        QueryCollectorArguments mockArguments = new QueryCollectorArguments.Builder().build();

        // When
        Optional<QueryCollectorContextSpec> result = QueryCollectorContextSpecRegistry.getQueryCollectorContextSpec(
            mockSearchContext,
            mockSearchContext.query(),
            mockArguments
        );

        // Then
        assertTrue(result.isEmpty());
    }

}
