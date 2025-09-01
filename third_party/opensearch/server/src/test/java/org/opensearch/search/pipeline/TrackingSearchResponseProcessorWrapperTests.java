/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline;

import org.density.action.search.SearchRequest;
import org.density.action.search.SearchResponse;
import org.density.core.action.ActionListener;
import org.density.search.SearchHits;
import org.density.test.DensityTestCase;
import org.junit.Before;

import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TrackingSearchResponseProcessorWrapperTests extends DensityTestCase {
    private SearchResponseProcessor mockProcessor;
    private TrackingSearchResponseProcessorWrapper wrapper;
    private PipelineProcessingContext context;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockProcessor = Mockito.mock(SearchResponseProcessor.class);
        wrapper = new TrackingSearchResponseProcessorWrapper(mockProcessor);
        context = new PipelineProcessingContext();
    }

    public void testConstructorThrowsExceptionWhenProcessorIsNull() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TrackingSearchResponseProcessorWrapper(null)
        );

        assertEquals("Wrapped processor cannot be null.", exception.getMessage());
    }

    public void testProcessResponseAsync() {
        SearchRequest mockRequest = new SearchRequest();
        SearchResponse inputResponse = Mockito.mock(SearchResponse.class);
        SearchResponse outputResponse = Mockito.mock(SearchResponse.class);

        when(inputResponse.getHits()).thenReturn(SearchHits.empty());
        when(outputResponse.getHits()).thenReturn(SearchHits.empty());

        wrapper.processResponseAsync(mockRequest, inputResponse, context, new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse result) {
                assertEquals(outputResponse, result);
                assertFalse(context.getProcessorExecutionDetails().isEmpty());
                ProcessorExecutionDetail detail = context.getProcessorExecutionDetails().get(0);
                assertEquals(wrapper.getType(), detail.getProcessorName());
                assertNotNull(detail.getInputData());
                assertNotNull(detail.getOutputData());
                assertEquals(ProcessorExecutionDetail.ProcessorStatus.SUCCESS, detail.getStatus());
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not trigger failure");
            }
        });

        verify(mockProcessor).processResponseAsync(eq(mockRequest), eq(inputResponse), eq(context), any());
    }
}
