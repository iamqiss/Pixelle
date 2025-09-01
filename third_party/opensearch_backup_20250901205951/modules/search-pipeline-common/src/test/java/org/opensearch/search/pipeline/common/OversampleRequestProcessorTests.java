/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline.common;

import org.density.action.search.SearchRequest;
import org.density.search.builder.SearchSourceBuilder;
import org.density.search.pipeline.PipelineProcessingContext;
import org.density.search.pipeline.common.helpers.ContextUtils;
import org.density.test.DensityTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OversampleRequestProcessorTests extends DensityTestCase {

    public void testEmptySource() {
        OversampleRequestProcessor.Factory factory = new OversampleRequestProcessor.Factory();
        Map<String, Object> config = new HashMap<>(Map.of(OversampleRequestProcessor.SAMPLE_FACTOR, 3.0));
        OversampleRequestProcessor processor = factory.create(Collections.emptyMap(), null, null, false, config, null);

        SearchRequest request = new SearchRequest();
        PipelineProcessingContext context = new PipelineProcessingContext();
        SearchRequest transformedRequest = processor.processRequest(request, context);
        assertEquals(request, transformedRequest);
        assertNull(context.getAttribute("original_size"));
    }

    public void testBasicBehavior() {
        OversampleRequestProcessor.Factory factory = new OversampleRequestProcessor.Factory();
        Map<String, Object> config = new HashMap<>(Map.of(OversampleRequestProcessor.SAMPLE_FACTOR, 3.0));
        OversampleRequestProcessor processor = factory.create(Collections.emptyMap(), null, null, false, config, null);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(10);
        SearchRequest request = new SearchRequest().source(sourceBuilder);
        PipelineProcessingContext context = new PipelineProcessingContext();
        SearchRequest transformedRequest = processor.processRequest(request, context);
        assertEquals(30, transformedRequest.source().size());
        assertEquals(10, context.getAttribute("original_size"));
    }

    public void testContextPrefix() {
        OversampleRequestProcessor.Factory factory = new OversampleRequestProcessor.Factory();
        Map<String, Object> config = new HashMap<>(
            Map.of(OversampleRequestProcessor.SAMPLE_FACTOR, 3.0, ContextUtils.CONTEXT_PREFIX_PARAMETER, "foo")
        );
        OversampleRequestProcessor processor = factory.create(Collections.emptyMap(), null, null, false, config, null);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().size(10);
        SearchRequest request = new SearchRequest().source(sourceBuilder);
        PipelineProcessingContext context = new PipelineProcessingContext();
        SearchRequest transformedRequest = processor.processRequest(request, context);
        assertEquals(30, transformedRequest.source().size());
        assertEquals(10, context.getAttribute("foo.original_size"));
    }
}
