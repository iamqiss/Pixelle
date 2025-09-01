/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.InputCoercionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.ExceptionsHelper;
import org.density.DensityException;
import org.density.core.compress.NotCompressedException;
import org.density.core.compress.NotXContentException;
import org.density.core.concurrency.DensityRejectedExecutionException;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

/**
 * Converts exceptions to a GRPC StatusRuntimeException.
 */
public class GrpcErrorHandler {
    private static final Logger logger = LogManager.getLogger(GrpcErrorHandler.class);

    private GrpcErrorHandler() {
        // Utility class, no instances
    }

    /**
     * Converts an exception to an appropriate GRPC StatusRuntimeException.
     * Uses shared constants from {@link ExceptionsHelper.ErrorMessages} and {@link ExceptionsHelper#summaryMessage}
     * for exact parity with HTTP error handling.
     *
     * @param e The exception to convert
     * @return StatusRuntimeException with appropriate GRPC status and HTTP-identical error messages
     */
    public static StatusRuntimeException convertToGrpcError(Exception e) {
        // ========== Density Business Logic Exceptions ==========
        // Custom Density exceptions which extend {@link DensityException}.
        // Uses {@link RestToGrpcStatusConverter} for REST -> gRPC status mapping and
        // follows {@link DensityException#generateFailureXContent} unwrapping logic
        if (e instanceof DensityException) {
            return handleDensityException((DensityException) e);
        }

        // ========== Density Core System Exceptions ==========
        // Low-level Density exceptions that don't extend DensityException - include full details
        else if (e instanceof DensityRejectedExecutionException) {
            return Status.RESOURCE_EXHAUSTED.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        } else if (e instanceof NotXContentException) {
            return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        } else if (e instanceof NotCompressedException) {
            return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        }

        // ========== 3. Third-party Library Exceptions ==========
        // External library exceptions (Jackson JSON parsing) - include full details
        else if (e instanceof InputCoercionException) {
            return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        } else if (e instanceof JsonParseException) {
            return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        }

        // ========== 4. Standard Java Exceptions ==========
        // Generic Java runtime exceptions - include full exception details for debugging
        else if (e instanceof IllegalArgumentException) {
            return Status.INVALID_ARGUMENT.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        } else if (e instanceof IllegalStateException) {
            return Status.FAILED_PRECONDITION.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        } else if (e instanceof SecurityException) {
            return Status.PERMISSION_DENIED.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        } else if (e instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        } else if (e instanceof InterruptedException) {
            return Status.CANCELLED.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        } else if (e instanceof IOException) {
            return Status.INTERNAL.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        }

        // ========== 5. Unknown/Unmapped Exceptions ==========
        // Safety fallback for any unexpected exception to {@code Status.INTERNAL} with full debugging info
        else {
            logger.warn("Unmapped exception type: {}, treating as INTERNAL error", e.getClass().getSimpleName());
            return Status.INTERNAL.withDescription(ExceptionsHelper.stackTrace(e)).asRuntimeException();
        }
    }

    /**
     * Handles Density-specific exceptions by converting their HTTP status to GRPC status.
     * Uses {@link ExceptionsHelper#summaryMessage(Throwable)} for exact parity with HTTP error handling.
     *
     * Uses {@link ExceptionsHelper#unwrapToDensityException(Throwable)} for shared unwrapping logic
     * with HTTP's {@link DensityException#generateFailureXContent}.
     *
     * @param e The {@link DensityException} to convert
     * @return StatusRuntimeException with mapped GRPC status and HTTP-identical error message
     */
    private static StatusRuntimeException handleDensityException(DensityException e) {
        Status grpcStatus = RestToGrpcStatusConverter.convertRestToGrpcStatus(e.status());

        Throwable unwrapped = ExceptionsHelper.unwrapToDensityException(e);

        String description = ExceptionsHelper.summaryMessage(unwrapped);
        return grpcStatus.withDescription(description).asRuntimeException();
    }

}
