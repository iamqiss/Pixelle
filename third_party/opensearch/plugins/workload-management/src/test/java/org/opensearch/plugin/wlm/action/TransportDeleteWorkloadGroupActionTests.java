/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.core.action.ActionListener;
import org.density.plugin.wlm.service.WorkloadGroupPersistenceService;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportDeleteWorkloadGroupActionTests extends DensityTestCase {

    ClusterService clusterService = mock(ClusterService.class);
    TransportService transportService = mock(TransportService.class);
    ActionFilters actionFilters = mock(ActionFilters.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    IndexNameExpressionResolver indexNameExpressionResolver = mock(IndexNameExpressionResolver.class);
    WorkloadGroupPersistenceService workloadGroupPersistenceService = mock(WorkloadGroupPersistenceService.class);

    TransportDeleteWorkloadGroupAction action = new TransportDeleteWorkloadGroupAction(
        clusterService,
        transportService,
        actionFilters,
        threadPool,
        indexNameExpressionResolver,
        workloadGroupPersistenceService
    );

    /**
     * Test case to validate the construction for TransportDeleteWorkloadGroupAction
     */
    public void testConstruction() {
        assertNotNull(action);
        assertEquals(ThreadPool.Names.SAME, action.executor());
    }

    /**
     * Test case to validate the clusterManagerOperation function in TransportDeleteWorkloadGroupAction
     */
    public void testClusterManagerOperation() throws Exception {
        DeleteWorkloadGroupRequest request = new DeleteWorkloadGroupRequest("testGroup");
        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        ClusterState clusterState = mock(ClusterState.class);
        action.clusterManagerOperation(request, clusterState, listener);
        verify(workloadGroupPersistenceService).deleteInClusterStateMetadata(eq(request), eq(listener));
    }
}
