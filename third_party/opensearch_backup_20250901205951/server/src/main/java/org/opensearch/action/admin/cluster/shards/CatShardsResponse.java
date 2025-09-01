/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.shards;

import org.density.Version;
import org.density.action.admin.indices.stats.IndicesStatsResponse;
import org.density.action.pagination.PageToken;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.ShardRouting;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A response of a cat shards request.
 *
 * @density.api
 */
public class CatShardsResponse extends ActionResponse {

    private IndicesStatsResponse indicesStatsResponse;
    private DiscoveryNodes nodes = DiscoveryNodes.EMPTY_NODES;
    private List<ShardRouting> responseShards = new ArrayList<>();
    private PageToken pageToken;

    public CatShardsResponse() {}

    public CatShardsResponse(StreamInput in) throws IOException {
        super(in);
        indicesStatsResponse = new IndicesStatsResponse(in);
        if (in.getVersion().onOrAfter(Version.V_2_18_0)) {
            nodes = DiscoveryNodes.readFrom(in, null);
            responseShards = in.readList(ShardRouting::new);
            if (in.readBoolean()) {
                pageToken = new PageToken(in);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indicesStatsResponse.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            nodes.writeToWithAttribute(out);
            out.writeList(responseShards);
            out.writeBoolean(pageToken != null);
            if (pageToken != null) {
                pageToken.writeTo(out);
            }
        }
    }

    public void setNodes(DiscoveryNodes nodes) {
        this.nodes = nodes;
    }

    public DiscoveryNodes getNodes() {
        return this.nodes;
    }

    public void setIndicesStatsResponse(IndicesStatsResponse indicesStatsResponse) {
        this.indicesStatsResponse = indicesStatsResponse;
    }

    public IndicesStatsResponse getIndicesStatsResponse() {
        return this.indicesStatsResponse;
    }

    public void setResponseShards(List<ShardRouting> responseShards) {
        this.responseShards = responseShards;
    }

    public List<ShardRouting> getResponseShards() {
        return this.responseShards;
    }

    public void setPageToken(PageToken pageToken) {
        this.pageToken = pageToken;
    }

    public PageToken getPageToken() {
        return this.pageToken;
    }
}
