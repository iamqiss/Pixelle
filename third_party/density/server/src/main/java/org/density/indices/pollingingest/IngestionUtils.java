/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.pollingingest;

import org.density.common.xcontent.XContentHelper;
import org.density.core.common.bytes.BytesArray;
import org.density.core.common.bytes.BytesReference;
import org.density.core.xcontent.MediaTypeRegistry;

import java.util.Map;

/**
 * Holds common utilities for streaming ingestion.
 */
public final class IngestionUtils {

    private IngestionUtils() {}

    public static Map<String, Object> getParsedPayloadMap(byte[] payload) {
        BytesReference payloadBR = new BytesArray(payload);
        Map<String, Object> payloadMap = XContentHelper.convertToMap(payloadBR, false, MediaTypeRegistry.xContentType(payloadBR)).v2();
        return payloadMap;
    }
}
