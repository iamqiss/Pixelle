/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.decommission;

import org.density.DensityException;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.rest.RestStatus;

import java.io.IOException;

/**
 * This exception is thrown if the node is decommissioned by @{@link DecommissionService}
 * and this nodes needs to be removed from the cluster
 *
 * @density.internal
 */
public class NodeDecommissionedException extends DensityException {

    public NodeDecommissionedException(String msg, Object... args) {
        super(msg, args);
    }

    public NodeDecommissionedException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.FAILED_DEPENDENCY;
    }
}
