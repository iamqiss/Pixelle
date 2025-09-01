/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.response.document.common;

import org.density.index.VersionType;

/**
 * Utility class for converting VersionType Protocol Buffers to Density VersionType objects.
 * This class handles the conversion of Protocol Buffer version type representations to their
 * corresponding Density version type enumerations.
 */
public class VersionTypeProtoUtils {

    private VersionTypeProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer VersionType to its corresponding Density VersionType.
     * Similar to {@link VersionType#fromString(String)}.
     *
     * @param versionType The Protocol Buffer VersionType to convert
     * @return The corresponding Density VersionType
     */
    public static VersionType fromProto(org.density.protobufs.VersionType versionType) {
        switch (versionType) {
            case VERSION_TYPE_EXTERNAL:
                return VersionType.EXTERNAL;
            case VERSION_TYPE_EXTERNAL_GTE:
                return VersionType.EXTERNAL_GTE;
            default:
                return VersionType.INTERNAL;
        }
    }
}
