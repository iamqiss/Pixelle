/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.ResourceNotFoundException;
import org.density.action.support.ActionFilters;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.core.action.ActionListener;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import static org.density.plugin.wlm.WorkloadManagementTestUtils.NAME_NONE_EXISTED;
import static org.density.plugin.wlm.WorkloadManagementTestUtils.NAME_ONE;
import static org.density.plugin.wlm.WorkloadManagementTestUtils.clusterState;
import static org.mockito.Mockito.mock;

public class TransportGetWorkloadGroupActionTests extends DensityTestCase {

    /**
     * Test case for ClusterManagerOperation function
     */
    @SuppressWarnings("unchecked")
    public void testClusterManagerOperation() throws Exception {
        GetWorkloadGroupRequest getWorkloadGroupRequest1 = new GetWorkloadGroupRequest(NAME_NONE_EXISTED);
        GetWorkloadGroupRequest getWorkloadGroupRequest2 = new GetWorkloadGroupRequest(NAME_ONE);
        TransportGetWorkloadGroupAction transportGetWorkloadGroupAction = new TransportGetWorkloadGroupAction(
            mock(ClusterService.class),
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ThreadPool.class),
            mock(IndexNameExpressionResolver.class)
        );
        assertThrows(
            ResourceNotFoundException.class,
            () -> transportGetWorkloadGroupAction.clusterManagerOperation(
                getWorkloadGroupRequest1,
                clusterState(),
                mock(ActionListener.class)
            )
        );
        transportGetWorkloadGroupAction.clusterManagerOperation(getWorkloadGroupRequest2, clusterState(), mock(ActionListener.class));
    }
}
