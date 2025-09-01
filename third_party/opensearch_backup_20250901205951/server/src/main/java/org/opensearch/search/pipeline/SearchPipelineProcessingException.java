/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline;

import org.density.DensityException;
import org.density.DensityWrapperException;
import org.density.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A dedicated wrapper for exceptions encountered executing a search pipeline processor. The wrapper is needed as we
 * currently only unwrap causes for instances of {@link DensityWrapperException}.
 *
 * @density.internal
 */
public class SearchPipelineProcessingException extends DensityException implements DensityWrapperException {
    SearchPipelineProcessingException(Exception cause) {
        super(cause);
    }

    public SearchPipelineProcessingException(StreamInput in) throws IOException {
        super(in);
    }
}
