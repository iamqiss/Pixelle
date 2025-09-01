/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.wlm;

import org.density.action.FailedNodeException;
import org.density.action.support.nodes.BaseNodesResponse;
import org.density.cluster.ClusterName;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.xcontent.XContentFactory;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;
import org.density.wlm.stats.WlmStats;
import org.density.wlm.stats.WorkloadGroupStats;

import java.io.IOException;
import java.util.List;

/**
 * A response for obtaining Workload Management Stats
 */
@ExperimentalApi
public class WlmStatsResponse extends BaseNodesResponse<WlmStats> implements ToXContentFragment {

    WlmStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public WlmStatsResponse(ClusterName clusterName, List<WlmStats> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<WlmStats> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(WlmStats::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<WlmStats> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (WlmStats wlmStats : getNodes()) {
            builder.startObject(wlmStats.getNode().getId());
            WorkloadGroupStats workloadGroupStats = wlmStats.getWorkloadGroupStats();
            workloadGroupStats.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.toString();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
