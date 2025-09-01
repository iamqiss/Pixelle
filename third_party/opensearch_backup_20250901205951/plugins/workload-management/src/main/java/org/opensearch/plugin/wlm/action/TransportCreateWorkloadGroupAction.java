/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

import static org.density.threadpool.ThreadPool.Names.SAME;

/**
 * Transport action to create WorkloadGroup
 *
 * @density.experimental
 */
public class TransportCreateWorkloadGroupAction extends TransportClusterManagerNodeAction<
    CreateWorkloadGroupRequest,
    CreateWorkloadGroupResponse> {

    private final WorkloadGroupPersistenceService workloadGroupPersistenceService;

    /**
     * Constructor for TransportCreateWorkloadGroupAction
     *
     * @param threadPool - {@link ThreadPool} object
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param indexNameExpressionResolver - {@link IndexNameExpressionResolver} object
     * @param workloadGroupPersistenceService - a {@link WorkloadGroupPersistenceService} object
     */
    @Inject
    public TransportCreateWorkloadGroupAction(
        ThreadPool threadPool,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        WorkloadGroupPersistenceService workloadGroupPersistenceService
    ) {
        super(
            CreateWorkloadGroupAction.NAME,
            transportService,
            workloadGroupPersistenceService.getClusterService(),
            threadPool,
            actionFilters,
            CreateWorkloadGroupRequest::new,
            indexNameExpressionResolver
        );
        this.workloadGroupPersistenceService = workloadGroupPersistenceService;
    }

    @Override
    protected void clusterManagerOperation(
        CreateWorkloadGroupRequest request,
        ClusterState clusterState,
        ActionListener<CreateWorkloadGroupResponse> listener
    ) {
        workloadGroupPersistenceService.persistInClusterStateMetadata(request.getWorkloadGroup(), listener);
    }

    @Override
    protected String executor() {
        return SAME;
    }

    @Override
    protected CreateWorkloadGroupResponse read(StreamInput in) throws IOException {
        return new CreateWorkloadGroupResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateWorkloadGroupRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
