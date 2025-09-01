/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.request.search;

import org.density.core.xcontent.XContentParser;
import org.density.protobufs.FieldCollapse;
import org.density.search.collapse.CollapseBuilder;

import java.io.IOException;

/**
 * Utility class for converting CollapseBuilder Protocol Buffers to Density objects.
 * This class provides methods to transform Protocol Buffer representations of field collapse
 * specifications into their corresponding Density CollapseBuilder implementations for
 * search result field collapsing and grouping.
 */
public class CollapseBuilderProtoUtils {

    private CollapseBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer FieldCollapse to an Density CollapseBuilder.
     * Similar to {@link CollapseBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * CollapseBuilder with the appropriate field, max concurrent group searches,
     * and inner hits settings.
     *
     * @param collapseProto The Protocol Buffer FieldCollapse to convert
     * @return A configured CollapseBuilder instance
     * @throws IOException if there's an error during parsing or conversion
     */
    protected static CollapseBuilder fromProto(FieldCollapse collapseProto) throws IOException {
        CollapseBuilder collapseBuilder = new CollapseBuilder(collapseProto.getField());

        if (collapseProto.hasMaxConcurrentGroupSearches()) {
            collapseBuilder.setMaxConcurrentGroupRequests(collapseProto.getMaxConcurrentGroupSearches());
        }
        if (collapseProto.getInnerHitsCount() > 0) {
            collapseBuilder.setInnerHits(InnerHitsBuilderProtoUtils.fromProto(collapseProto.getInnerHitsList()));
        }

        return collapseBuilder;
    }
}
