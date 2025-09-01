/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.engine;

import org.density.DensityException;
import org.density.DensityWrapperException;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.rest.RestStatus;

import java.io.IOException;

/**
 * Exception thrown when there is an error in the ingestion engine.
 *
 * @density.internal
 */
public class IngestionEngineException extends DensityException implements DensityWrapperException {
    public IngestionEngineException(String message) {
        super(message);
    }

    public IngestionEngineException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
