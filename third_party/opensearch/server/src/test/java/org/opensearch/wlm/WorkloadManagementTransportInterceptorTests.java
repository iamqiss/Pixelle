/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm;

import org.density.cluster.ClusterState;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.service.ClusterService;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportRequest;
import org.density.transport.TransportRequestHandler;
import org.density.wlm.WorkloadManagementTransportInterceptor.RequestHandler;
import org.density.wlm.cancellation.WorkloadGroupTaskCancellationService;

import java.util.Collections;

import static org.density.threadpool.ThreadPool.Names.SAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkloadManagementTransportInterceptorTests extends DensityTestCase {
    private WorkloadGroupTaskCancellationService mockTaskCancellationService;
    private ClusterService mockClusterService;
    private ThreadPool mockThreadPool;
    private WorkloadManagementSettings mockWorkloadManagementSettings;
    private ThreadPool threadPool;
    private WorkloadManagementTransportInterceptor sut;
    private WorkloadGroupsStateAccessor stateAccessor;

    public void setUp() throws Exception {
        super.setUp();
        mockTaskCancellationService = mock(WorkloadGroupTaskCancellationService.class);
        mockClusterService = mock(ClusterService.class);
        mockThreadPool = mock(ThreadPool.class);
        mockWorkloadManagementSettings = mock(WorkloadManagementSettings.class);
        threadPool = new TestThreadPool(getTestName());
        stateAccessor = new WorkloadGroupsStateAccessor();

        ClusterState state = mock(ClusterState.class);
        Metadata metadata = mock(Metadata.class);
        when(mockClusterService.state()).thenReturn(state);
        when(state.metadata()).thenReturn(metadata);
        when(metadata.workloadGroups()).thenReturn(Collections.emptyMap());
        sut = new WorkloadManagementTransportInterceptor(
            threadPool,
            new WorkloadGroupService(
                mockTaskCancellationService,
                mockClusterService,
                mockThreadPool,
                mockWorkloadManagementSettings,
                stateAccessor
            )
        );
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testInterceptHandler() {
        TransportRequestHandler<TransportRequest> requestHandler = sut.interceptHandler("Search", SAME, false, null);
        assertTrue(requestHandler instanceof RequestHandler);
    }
}
