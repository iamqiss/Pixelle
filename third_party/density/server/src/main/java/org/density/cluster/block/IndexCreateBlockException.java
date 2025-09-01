/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.block;

import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Set;

/**
 * Internal exception on obtaining an index create block
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public class IndexCreateBlockException extends ClusterBlockException {

    public IndexCreateBlockException(Set<ClusterBlock> globalLevelBlocks) {
        super(globalLevelBlocks);
    }

    public IndexCreateBlockException(StreamInput in) throws IOException {
        super(in);
    }
}
