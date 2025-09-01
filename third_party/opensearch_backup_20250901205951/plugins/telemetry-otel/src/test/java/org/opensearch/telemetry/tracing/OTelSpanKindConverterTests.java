/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.telemetry.tracing;

import org.density.test.DensityTestCase;

import io.opentelemetry.api.trace.SpanKind;

public class OTelSpanKindConverterTests extends DensityTestCase {

    public void testSpanKindNullConverterNull() {
        assertEquals(SpanKind.INTERNAL, OTelSpanKindConverter.convert(null));
    }

    public void testSpanKindConverter() {
        assertEquals(SpanKind.INTERNAL, OTelSpanKindConverter.convert(org.density.telemetry.tracing.SpanKind.INTERNAL));
        assertEquals(SpanKind.CLIENT, OTelSpanKindConverter.convert(org.density.telemetry.tracing.SpanKind.CLIENT));
        assertEquals(SpanKind.SERVER, OTelSpanKindConverter.convert(org.density.telemetry.tracing.SpanKind.SERVER));
    }

}
