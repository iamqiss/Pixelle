/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest;

import org.density.common.annotation.PublicApi;

import java.util.Set;

/**
 * A collection of REST method handlers.
 */
@PublicApi(since = "2.12.0")
public interface MethodHandlers {
    /**
     * Return a set of all valid HTTP methods for the particular path.
     */
    Set<RestRequest.Method> getValidMethods();

    /**
     * Returns the relative HTTP path of the set of method handlers.
     */
    String getPath();
}
