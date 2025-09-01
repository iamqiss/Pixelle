/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugins;

import org.density.common.annotation.ExperimentalApi;

/**
 * An exception handler for errors that might happen while secure transport handle the requests.
 *
 * @see <a href="https://github.com/density-project/security/blob/main/src/main/java/org/density/security/ssl/SslExceptionHandler.java">SslExceptionHandler</a>
 *
 * @density.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface TransportExceptionHandler {
    static TransportExceptionHandler NOOP = t -> {};

    /**
     * Handler for errors happening during the server side processing of the requests
     * @param t the error
     */
    void onError(Throwable t);
}
