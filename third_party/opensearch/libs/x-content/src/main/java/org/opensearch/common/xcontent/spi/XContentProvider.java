/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.xcontent.spi;

import org.density.common.xcontent.XContentType;
import org.density.core.xcontent.MediaType;
import org.density.core.xcontent.spi.MediaTypeProvider;

import java.util.List;
import java.util.Map;

/**
 * Media Type implementations provided by xcontent library
 *
 * @density.internal
 */
public class XContentProvider implements MediaTypeProvider {
    /** Returns the concrete {@link MediaType} provided by the xcontent library */
    @Override
    public List<MediaType> getMediaTypes() {
        return List.of(XContentType.values());
    }

    /** Returns the additional {@link MediaType} aliases provided by the xcontent library */
    @Override
    public Map<String, MediaType> getAdditionalMediaTypes() {
        return Map.of("application/*", XContentType.JSON, "application/x-ndjson", XContentType.JSON);
    }
}
