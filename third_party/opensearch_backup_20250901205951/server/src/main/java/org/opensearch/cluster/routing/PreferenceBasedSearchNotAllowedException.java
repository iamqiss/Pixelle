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
 * Thrown to disallow preference based search with strict weighted shard routing. See {@link WeightedRoutingService}
 * * for more details.
 *
 * @density.internal
 */
public class PreferenceBasedSearchNotAllowedException extends DensityException {

    public PreferenceBasedSearchNotAllowedException(StreamInput in) throws IOException {
        super(in);
    }

    public PreferenceBasedSearchNotAllowedException(String msg, Object... args) {
        super(msg, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.FORBIDDEN;
    }
}
