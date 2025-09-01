/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.util;

import org.density.common.UUIDs;

/**
 * Common utility methods for request handling.
 *
 * @density.internal
 */
public final class RequestUtils {

    private RequestUtils() {}

    /**
     * Generates a new ID field for new documents.
     */
    public static String generateID() {
        return UUIDs.base64UUID();
    }
}
