/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing.sampler;

import java.util.List;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;

import static org.density.telemetry.tracing.AttributeNames.TRACE;

/**
 * RequestSampler based on HeadBased sampler
 */
public class RequestSampler implements Sampler {
    private final Sampler fallbackSampler;

    /**
     * Creates request sampler which applies based on all applicable sampler
     * @param fallbackSampler Sampler
     */
    public RequestSampler(Sampler fallbackSampler) {
        this.fallbackSampler = fallbackSampler;
    }

    @Override
    public SamplingResult shouldSample(
        Context parentContext,
        String traceId,
        String name,
        SpanKind spanKind,
        Attributes attributes,
        List<LinkData> parentLinks
    ) {
        final String trace = attributes.get(AttributeKey.stringKey(TRACE));

        if (trace != null) {
            return (Boolean.parseBoolean(trace) == true) ? SamplingResult.recordAndSample() : SamplingResult.drop();
        }
        if (fallbackSampler != null) {
            return fallbackSampler.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
        }
        return SamplingResult.recordAndSample();
    }

    @Override
    public String getDescription() {
        return "Request Sampler";
    }

    @Override
    public String toString() {
        return getDescription();
    }
}
