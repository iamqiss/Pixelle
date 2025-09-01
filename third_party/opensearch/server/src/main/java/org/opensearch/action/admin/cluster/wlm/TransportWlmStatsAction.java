/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.wlm;

import org.density.action.FailedNodeException;
import org.density.action.support.ActionFilters;
import org.density.action.support.nodes.TransportNodesAction;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.wlm.WorkloadGroupService;
import org.density.wlm.stats.WlmStats;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for obtaining WlmStats
 *
 * @density.experimental
 */
public class TransportWlmStatsAction extends TransportNodesAction<WlmStatsRequest, WlmStatsResponse, WlmStatsRequest, WlmStats> {

    final WorkloadGroupService workloadGroupService;

    @Inject
    public TransportWlmStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        WorkloadGroupService workloadGroupService,
        ActionFilters actionFilters
    ) {
        super(
            WlmStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            WlmStatsRequest::new,
            WlmStatsRequest::new,
            ThreadPool.Names.MANAGEMENT,
            WlmStats.class
        );
        this.workloadGroupService = workloadGroupService;
    }

    @Override
    protected WlmStatsResponse newResponse(WlmStatsRequest request, List<WlmStats> wlmStats, List<FailedNodeException> failures) {
        return new WlmStatsResponse(clusterService.getClusterName(), wlmStats, failures);
    }

    @Override
    protected WlmStatsRequest newNodeRequest(WlmStatsRequest request) {
        return request;
    }

    @Override
    protected WlmStats newNodeResponse(StreamInput in) throws IOException {
        return new WlmStats(in);
    }

    @Override
    protected WlmStats nodeOperation(WlmStatsRequest wlmStatsRequest) {
        assert transportService.getLocalNode() != null;
        return new WlmStats(
            transportService.getLocalNode(),
            workloadGroupService.nodeStats(wlmStatsRequest.getWorkloadGroupIds(), wlmStatsRequest.isBreach())
        );
    }
}
