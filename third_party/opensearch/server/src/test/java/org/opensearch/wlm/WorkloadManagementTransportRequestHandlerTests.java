/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.wlm;

import org.density.action.index.IndexRequest;
import org.density.core.concurrency.DensityRejectedExecutionException;
import org.density.search.internal.ShardSearchRequest;
import org.density.tasks.Task;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportChannel;
import org.density.transport.TransportRequest;
import org.density.transport.TransportRequestHandler;

import java.util.Collections;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class WorkloadManagementTransportRequestHandlerTests extends DensityTestCase {
    private WorkloadManagementTransportInterceptor.RequestHandler<TransportRequest> sut;
    private ThreadPool threadPool;
    private WorkloadGroupService workloadGroupService;

    private TestTransportRequestHandler<TransportRequest> actualHandler;

    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        actualHandler = new TestTransportRequestHandler<>();
        workloadGroupService = mock(WorkloadGroupService.class);

        sut = new WorkloadManagementTransportInterceptor.RequestHandler<>(threadPool, actualHandler, workloadGroupService);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testMessageReceivedForSearchWorkload_nonRejectionCase() throws Exception {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        WorkloadGroupTask spyTask = getSpyTask();
        doNothing().when(workloadGroupService).rejectIfNeeded(anyString());
        sut.messageReceived(request, mock(TransportChannel.class), spyTask);
        assertTrue(sut.isSearchWorkloadRequest(spyTask));
    }

    public void testMessageReceivedForSearchWorkload_RejectionCase() throws Exception {
        ShardSearchRequest request = mock(ShardSearchRequest.class);
        WorkloadGroupTask spyTask = getSpyTask();
        doThrow(DensityRejectedExecutionException.class).when(workloadGroupService).rejectIfNeeded(anyString());

        assertThrows(DensityRejectedExecutionException.class, () -> sut.messageReceived(request, mock(TransportChannel.class), spyTask));
    }

    public void testMessageReceivedForNonSearchWorkload() throws Exception {
        IndexRequest indexRequest = mock(IndexRequest.class);
        Task task = mock(Task.class);
        sut.messageReceived(indexRequest, mock(TransportChannel.class), task);
        assertFalse(sut.isSearchWorkloadRequest(task));
        assertEquals(1, actualHandler.invokeCount);
    }

    private static WorkloadGroupTask getSpyTask() {
        final WorkloadGroupTask task = new WorkloadGroupTask(123, "transport", "Search", "test task", null, Collections.emptyMap());

        return spy(task);
    }

    private static class TestTransportRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {
        int invokeCount = 0;

        @Override
        public void messageReceived(TransportRequest request, TransportChannel channel, Task task) throws Exception {
            invokeCount += 1;
        }
    };
}
