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
 * This exception is thrown if the node is weighed away by @{@link WeightedRoutingService}
 *
 * @density.internal
 */
public class NodeWeighedAwayException extends DensityException {

    public NodeWeighedAwayException(StreamInput in) throws IOException {
        super(in);
    }

    public NodeWeighedAwayException(String msg, Object... args) {
        super(msg, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.MISDIRECTED_REQUEST;
    }
}
