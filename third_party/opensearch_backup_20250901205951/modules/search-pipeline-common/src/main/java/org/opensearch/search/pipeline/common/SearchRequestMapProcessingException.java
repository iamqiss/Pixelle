/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline.common;

import org.density.DensityException;
import org.density.DensityWrapperException;

/**
 * An exception that indicates an error occurred while processing a {@link SearchRequestMap}.
 */
class SearchRequestMapProcessingException extends DensityException implements DensityWrapperException {

    /**
     * Constructs a new SearchRequestMapProcessingException with the specified message.
     *
     * @param msg  The error message.
     * @param args Arguments to substitute in the error message.
     */
    public SearchRequestMapProcessingException(String msg, Object... args) {
        super(msg, args);
    }

    /**
     * Constructs a new SearchRequestMapProcessingException with the specified message and cause.
     *
     * @param msg   The error message.
     * @param cause The cause of the exception.
     * @param args  Arguments to substitute in the error message.
     */
    public SearchRequestMapProcessingException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }
}
