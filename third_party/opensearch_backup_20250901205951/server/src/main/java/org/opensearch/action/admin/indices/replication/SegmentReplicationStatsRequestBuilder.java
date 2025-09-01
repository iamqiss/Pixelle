/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.replication;

import org.density.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.density.common.annotation.PublicApi;
import org.density.transport.client.DensityClient;

/**
 * Segment Replication stats information request builder.
 *
  * @density.api
 */
@PublicApi(since = "1.0.0")
public class SegmentReplicationStatsRequestBuilder extends BroadcastOperationRequestBuilder<
    SegmentReplicationStatsRequest,
    SegmentReplicationStatsResponse,
    SegmentReplicationStatsRequestBuilder> {

    public SegmentReplicationStatsRequestBuilder(DensityClient client, SegmentReplicationStatsAction action) {
        super(client, action, new SegmentReplicationStatsRequest());
    }

    public SegmentReplicationStatsRequestBuilder setDetailed(boolean detailed) {
        request.detailed(detailed);
        return this;
    }

    public SegmentReplicationStatsRequestBuilder setActiveOnly(boolean activeOnly) {
        request.activeOnly(activeOnly);
        return this;
    }

    public SegmentReplicationStatsRequestBuilder shards(String... indices) {
        request.shards(indices);
        return this;
    }

}
