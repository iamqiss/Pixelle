/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.routing;

import org.density.DensityException;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.rest.RestStatus;

import java.io.IOException;

/**
 * Thrown when failing to update the routing weight due to an unsupported state. See {@link WeightedRoutingService} for more details.
 *
 * @density.internal
 */
public class UnsupportedWeightedRoutingStateException extends DensityException {
    public UnsupportedWeightedRoutingStateException(StreamInput in) throws IOException {
        super(in);
    }

    public UnsupportedWeightedRoutingStateException(String msg, Object... args) {
        super(msg, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }
}
