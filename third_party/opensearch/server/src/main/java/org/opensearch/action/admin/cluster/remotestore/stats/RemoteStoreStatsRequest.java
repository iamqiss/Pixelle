/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.stats;

import org.density.action.support.broadcast.BroadcastRequest;
import org.density.common.annotation.PublicApi;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Encapsulates all remote store stats
 *
 * @density.api
 */
@PublicApi(since = "2.8.0")
public class RemoteStoreStatsRequest extends BroadcastRequest<RemoteStoreStatsRequest> {

    private String[] shards;
    private boolean local = false;

    public RemoteStoreStatsRequest() {
        super((String[]) null);
        shards = new String[0];
    }

    public RemoteStoreStatsRequest(StreamInput in) throws IOException {
        super(in);
        shards = in.readStringArray();
        local = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(shards);
        out.writeBoolean(local);
    }

    public RemoteStoreStatsRequest shards(String... shards) {
        this.shards = shards;
        return this;
    }

    public String[] shards() {
        return this.shards;
    }

    public void local(boolean local) {
        this.local = local;
    }

    public boolean local() {
        return local;
    }
}
