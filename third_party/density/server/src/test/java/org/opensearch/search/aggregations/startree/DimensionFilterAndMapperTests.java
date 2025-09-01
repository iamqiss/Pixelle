/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.aggregations.startree;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.density.index.compositeindex.datacube.Metric;
import org.density.index.compositeindex.datacube.MetricStat;
import org.density.index.compositeindex.datacube.OrdinalDimension;
import org.density.index.compositeindex.datacube.startree.StarTreeField;
import org.density.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.density.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.density.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.density.index.mapper.CompositeDataCubeFieldType;
import org.density.index.mapper.IpFieldMapper;
import org.density.index.mapper.KeywordFieldMapper;
import org.density.index.mapper.MappedFieldType;
import org.density.index.mapper.MapperService;
import org.density.index.mapper.NumberFieldMapper;
import org.density.index.mapper.StarTreeMapper;
import org.density.index.mapper.WildcardFieldMapper;
import org.density.index.query.BoolQueryBuilder;
import org.density.index.query.QueryBuilder;
import org.density.index.query.RangeQueryBuilder;
import org.density.index.query.TermQueryBuilder;
import org.density.index.query.TermsQueryBuilder;
import org.density.search.internal.SearchContext;
import org.density.search.startree.filter.DimensionFilter;
import org.density.search.startree.filter.DimensionFilter.MatchType;
import org.density.search.startree.filter.MatchNoneFilter;
import org.density.search.startree.filter.StarTreeFilter;
import org.density.search.startree.filter.provider.DimensionFilterMapper;
import org.density.search.startree.filter.provider.StarTreeFilterProvider;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DimensionFilterAndMapperTests extends DensityTestCase {

    public void testIpMapping() throws Exception {
        MappedFieldType mappedFieldType = new IpFieldMapper.IpFieldType("ip_field");
        BytesRef ipAsBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName("192.168.1.1")));
        testOrdinalMapping(mappedFieldType, ipAsBytes);
    }

    public void testRawValuesIpParsing() throws UnknownHostException {
        SearchContext searchContext = mock(SearchContext.class);
        MappedFieldType mappedFieldType = new IpFieldMapper.IpFieldType("ip_field");
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType, searchContext);

        assertThrows(IllegalArgumentException.class, () -> dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of(1.0f)));
        assertThrows(
            IllegalArgumentException.class,
            () -> dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of("not.a.valid.ip"))
        );
        DimensionFilter df = dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of(InetAddress.getByName("192.168.1.1")));
        assertEquals("ip_field", df.getMatchingDimension());
    }

    public void testKeywordOrdinalMapping() throws IOException {
        MappedFieldType mappedFieldType = new KeywordFieldMapper.KeywordFieldType("keyword");
        BytesRef bytesRef = new BytesRef(new byte[] { 17, 29 });
        testOrdinalMapping(mappedFieldType, bytesRef);
    }

    private void testOrdinalMapping(final MappedFieldType mappedFieldType, final BytesRef bytesRef) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType, searchContext);
        StarTreeValues starTreeValues = mock(StarTreeValues.class);
        SortedSetStarTreeValuesIterator sortedSetStarTreeValuesIterator = mock(SortedSetStarTreeValuesIterator.class);
        TermsEnum termsEnum = mock(TermsEnum.class);
        when(sortedSetStarTreeValuesIterator.termsEnum()).thenReturn(termsEnum);
        when(starTreeValues.getDimensionValuesIterator("field")).thenReturn(sortedSetStarTreeValuesIterator);
        Optional<Long> matchingOrdinal;

        // Case Exact Match and found
        when(sortedSetStarTreeValuesIterator.lookupTerm(bytesRef)).thenReturn(1L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.EXACT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(1, (long) matchingOrdinal.get());

        // Case Exact Match and not found
        when(sortedSetStarTreeValuesIterator.lookupTerm(bytesRef)).thenReturn(-10L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.EXACT);
        assertFalse(matchingOrdinal.isPresent());

        // Case GTE -> FOUND and NOT_FOUND
        for (TermsEnum.SeekStatus seekStatus : new TermsEnum.SeekStatus[] { TermsEnum.SeekStatus.FOUND, TermsEnum.SeekStatus.NOT_FOUND }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(seekStatus);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GTE);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(10L, (long) matchingOrdinal.get());
        }

        // Seek Status END is same for GTE, GT
        for (MatchType matchType : new MatchType[] { MatchType.GT, MatchType.GTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.END);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertFalse(matchingOrdinal.isPresent());
        }

        // Case GT -> FOUND and matched
        when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.FOUND);
        when(sortedSetStarTreeValuesIterator.getValueCount()).thenReturn(2L);
        when(termsEnum.ord()).thenReturn(0L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(1L, (long) matchingOrdinal.get());
        // Case GT -> FOUND and unmatched
        when(termsEnum.ord()).thenReturn(3L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertFalse(matchingOrdinal.isPresent());

        // Case GT -> NOT_FOUND
        when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.NOT_FOUND);
        when(termsEnum.ord()).thenReturn(10L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(10L, (long) matchingOrdinal.get());

        // Seek Status END is same for LTE, LT
        for (MatchType matchType : new MatchType[] { MatchType.LT, MatchType.LTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.END);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(10L, (long) matchingOrdinal.get());
        }

        // Seek Status NOT_FOUND is same for LTE, LT
        for (MatchType matchType : new MatchType[] { MatchType.LT, MatchType.LTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.NOT_FOUND);
            when(sortedSetStarTreeValuesIterator.getValueCount()).thenReturn(2L);
            when(termsEnum.ord()).thenReturn(1L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(0L, (long) matchingOrdinal.get());
            // Case unmatched
            when(termsEnum.ord()).thenReturn(0L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertFalse(matchingOrdinal.isPresent());
        }
    }

    public void testStarTreeFilterProviders() throws IOException {
        CompositeDataCubeFieldType compositeDataCubeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(new OrdinalDimension("keyword"), new OrdinalDimension("status"), new OrdinalDimension("method")),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                new StarTreeFieldConfiguration(
                    randomIntBetween(1, 10_000),
                    Collections.emptySet(),
                    StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
                )
            )
        );
        MapperService mapperService = mock(MapperService.class);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        // Null returned when mapper doesn't exist
        assertNull(DimensionFilterMapper.Factory.fromMappedFieldType(new WildcardFieldMapper.WildcardFieldType("field"), searchContext));

        // Null returned for no mapped field type
        assertNull(DimensionFilterMapper.Factory.fromMappedFieldType(null, searchContext));

        // Provider for null Query builder
        assertEquals(StarTreeFilterProvider.MATCH_ALL_PROVIDER, StarTreeFilterProvider.SingletonFactory.getProvider(null));

        QueryBuilder[] queryBuilders = new QueryBuilder[] {
            new TermQueryBuilder("field", "value"),
            new TermsQueryBuilder("field", List.of("value")),
            new RangeQueryBuilder("field") };

        for (QueryBuilder queryBuilder : queryBuilders) {
            // Dimension Not Found
            StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(queryBuilder);
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));
        }

        queryBuilders = new QueryBuilder[] {
            new TermQueryBuilder("keyword", "value"),
            new TermsQueryBuilder("keyword", List.of("value")),
            new RangeQueryBuilder("keyword") };

        for (QueryBuilder queryBuilder : queryBuilders) {
            // Mapped field type not supported
            StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(queryBuilder);
            when(mapperService.fieldType("keyword")).thenReturn(new WildcardFieldMapper.WildcardFieldType("keyword"));
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));

            // Unsupported Mapped Type
            when(mapperService.fieldType("keyword")).thenReturn(null);
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));
        }

        // Testing MatchNoneFilter
        DimensionFilter dimensionFilter = new MatchNoneFilter();
        dimensionFilter.initialiseForSegment(null, null);
        ArrayBasedCollector collector = new ArrayBasedCollector();
        assertFalse(dimensionFilter.matchDimValue(1, null));
        dimensionFilter.matchStarTreeNodes(null, null, collector);
        assertEquals(0, collector.collectedNodeCount());

        // Setup common field types
        KeywordFieldMapper.KeywordFieldType methodType = new KeywordFieldMapper.KeywordFieldType("method");
        NumberFieldMapper.NumberFieldType statusType = new NumberFieldMapper.NumberFieldType(
            "status",
            NumberFieldMapper.NumberType.INTEGER
        );
        when(mapperService.fieldType("method")).thenReturn(methodType);
        when(mapperService.fieldType("status")).thenReturn(statusType);

        // Test simple MUST clause
        BoolQueryBuilder simpleMustQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new TermQueryBuilder("status", 200));
        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(simpleMustQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, simpleMustQuery, compositeDataCubeFieldType);
        assertNotNull(filter);
        assertEquals(2, filter.getDimensions().size());
        assertTrue(filter.getDimensions().contains("method"));
        assertTrue(filter.getDimensions().contains("status"));

        // Test MUST with nested SHOULD on different dimension
        BoolQueryBuilder mustWithShould = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 200)).should(new TermQueryBuilder("status", 404)));
        filter = provider.getFilter(searchContext, mustWithShould, compositeDataCubeFieldType);
        assertNotNull(filter);
        assertEquals(2, filter.getDimensions().size());
        assertEquals(1, filter.getFiltersForDimension("method").size());
        assertEquals(2, filter.getFiltersForDimension("status").size());

        // Test invalid SHOULD across dimensions
        BoolQueryBuilder invalidShould = new BoolQueryBuilder().should(new TermQueryBuilder("method", "GET"))
            .should(new TermQueryBuilder("status", 200));
        assertNull(provider.getFilter(searchContext, invalidShould, compositeDataCubeFieldType));

        // Test MUST with invalid nested SHOULD
        BoolQueryBuilder invalidNestedShould = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("method", "POST")).should(new TermQueryBuilder("status", 200)));
        assertNull(provider.getFilter(searchContext, invalidNestedShould, compositeDataCubeFieldType));

        // Test MUST with SHOULD on same dimension
        BoolQueryBuilder mustWithSameDimShould = new BoolQueryBuilder().must(new TermQueryBuilder("status", 200))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 404)).should(new TermQueryBuilder("status", 500)));
        assertNull(provider.getFilter(searchContext, mustWithSameDimShould, compositeDataCubeFieldType));
    }
}
