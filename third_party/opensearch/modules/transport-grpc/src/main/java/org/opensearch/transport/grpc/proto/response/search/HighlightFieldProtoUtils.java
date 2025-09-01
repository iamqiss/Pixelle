/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.density.transport.grpc.proto.response.search;

import org.density.core.common.text.Text;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.protobufs.StringArray;
import org.density.search.fetch.subphase.highlight.HighlightField;

/**
 * Utility class for converting HighlightField objects to Protocol Buffers.
 * This class handles the conversion of document get operation results to their
 * Protocol Buffer representation.
 */
public class HighlightFieldProtoUtils {

    private HighlightFieldProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a HighlightField values (list of objects) to its Protocol Buffer representation.
     * This method is equivalent to the  {@link HighlightField#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param fragments The list of HighlightField values to convert
     * @return A Protobuf Value representation
     */
    protected static StringArray toProto(Text[] fragments) {
        StringArray.Builder stringArrayBuilder = StringArray.newBuilder();
        for (Text text : fragments) {
            stringArrayBuilder.addStringArray(text.string());
        }
        return stringArrayBuilder.build();
    }
}
