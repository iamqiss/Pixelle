/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline.common;

import org.density.action.search.SearchRequest;
import org.density.ingest.ConfigurationUtils;
import org.density.search.SearchService;
import org.density.search.pipeline.AbstractProcessor;
import org.density.search.pipeline.PipelineProcessingContext;
import org.density.search.pipeline.Processor;
import org.density.search.pipeline.SearchRequestProcessor;
import org.density.search.pipeline.StatefulSearchRequestProcessor;
import org.density.search.pipeline.common.helpers.ContextUtils;

import java.util.Map;

import static org.density.search.pipeline.common.helpers.ContextUtils.applyContextPrefix;

/**
 * Multiplies the "size" parameter on the {@link SearchRequest} by the given scaling factor, storing the original value
 * in the request context as "original_size".
 */
public class OversampleRequestProcessor extends AbstractProcessor implements StatefulSearchRequestProcessor {

    /**
     * Key to reference this processor type from a search pipeline.
     */
    public static final String TYPE = "oversample";
    static final String SAMPLE_FACTOR = "sample_factor";
    static final String ORIGINAL_SIZE = "original_size";
    private final double sampleFactor;
    private final String contextPrefix;

    private OversampleRequestProcessor(String tag, String description, boolean ignoreFailure, double sampleFactor, String contextPrefix) {
        super(tag, description, ignoreFailure);
        this.sampleFactor = sampleFactor;
        this.contextPrefix = contextPrefix;
    }

    @Override
    public SearchRequest processRequest(SearchRequest request, PipelineProcessingContext requestContext) {
        if (request.source() != null) {
            int originalSize = request.source().size();
            if (originalSize == -1) {
                originalSize = SearchService.DEFAULT_SIZE;
            }
            requestContext.setAttribute(applyContextPrefix(contextPrefix, ORIGINAL_SIZE), originalSize);
            int newSize = (int) Math.ceil(originalSize * sampleFactor);
            request.source().size(newSize);
        }
        return request;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    static class Factory implements Processor.Factory<SearchRequestProcessor> {
        @Override
        public OversampleRequestProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) {
            double sampleFactor = ConfigurationUtils.readDoubleProperty(TYPE, tag, config, SAMPLE_FACTOR);
            if (sampleFactor < 1.0) {
                throw ConfigurationUtils.newConfigurationException(TYPE, tag, SAMPLE_FACTOR, "Value must be >= 1.0");
            }
            String contextPrefix = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, ContextUtils.CONTEXT_PREFIX_PARAMETER);
            return new OversampleRequestProcessor(tag, description, ignoreFailure, sampleFactor, contextPrefix);
        }
    }
}
