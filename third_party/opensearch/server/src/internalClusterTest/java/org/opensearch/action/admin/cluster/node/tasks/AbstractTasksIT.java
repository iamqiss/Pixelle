/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.node.tasks;

import org.density.ExceptionsHelper;
import org.density.ResourceNotFoundException;
import org.density.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.density.action.support.WriteRequest;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.service.ClusterService;
import org.density.common.collect.Tuple;
import org.density.common.settings.Settings;
import org.density.core.common.Strings;
import org.density.core.tasks.TaskId;
import org.density.core.tasks.resourcetracker.ThreadResourceInfo;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.plugins.Plugin;
import org.density.tasks.TaskInfo;
import org.density.test.DensityIntegTestCase;
import org.density.test.tasks.MockTaskManager;
import org.density.test.transport.MockTransportService;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Base IT test class for Tasks ITs
 */
abstract class AbstractTasksIT extends DensityIntegTestCase {

    protected Map<Tuple<String, String>, RecordingTaskManagerListener> listeners = new HashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Collection<Class<? extends Plugin>> mockPlugins = new ArrayList<>(super.getMockPlugins());
        mockPlugins.remove(MockTransportService.TestPlugin.class);
        return mockPlugins;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, TestTaskPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.getKey(), true)
            .build();
    }

    @Override
    public void tearDown() throws Exception {
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            ((MockTaskManager) internalCluster().getInstance(TransportService.class, entry.getKey().v1()).getTaskManager()).removeListener(
                entry.getValue()
            );
        }
        listeners.clear();
        super.tearDown();
    }

    /**
     * Registers recording task event listeners with the given action mask on all nodes
     */
    protected void registerTaskManagerListeners(String actionMasks) {
        for (String nodeName : internalCluster().getNodeNames()) {
            DiscoveryNode node = internalCluster().getInstance(ClusterService.class, nodeName).localNode();
            RecordingTaskManagerListener listener = new RecordingTaskManagerListener(node.getId(), actionMasks.split(","));
            ((MockTaskManager) internalCluster().getInstance(TransportService.class, nodeName).getTaskManager()).addListener(listener);
            RecordingTaskManagerListener oldListener = listeners.put(new Tuple<>(node.getName(), actionMasks), listener);
            assertNull(oldListener);
        }
    }

    /**
     * Resets all recording task event listeners with the given action mask on all nodes
     */
    protected void resetTaskManagerListeners(String actionMasks) {
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            if (actionMasks == null || entry.getKey().v2().equals(actionMasks)) {
                entry.getValue().reset();
            }
        }
    }

    /**
     * Returns the number of events that satisfy the criteria across all nodes
     *
     * @param actionMasks action masks to match
     * @return number of events that satisfy the criteria
     */
    protected int numberOfEvents(String actionMasks, Function<Tuple<Boolean, TaskInfo>, Boolean> criteria) {
        return findEvents(actionMasks, criteria).size();
    }

    /**
     * Returns all events that satisfy the criteria across all nodes
     *
     * @param actionMasks action masks to match
     * @return List of events that satisfy the criteria
     */
    protected List<TaskInfo> findEvents(String actionMasks, Function<Tuple<Boolean, TaskInfo>, Boolean> criteria) {
        List<TaskInfo> events = new ArrayList<>();
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            if (actionMasks == null || entry.getKey().v2().equals(actionMasks)) {
                for (Tuple<Boolean, TaskInfo> taskEvent : entry.getValue().getEvents()) {
                    if (criteria.apply(taskEvent)) {
                        events.add(taskEvent.v2());
                    }
                }
            }
        }
        return events;
    }

    protected Map<Long, List<ThreadResourceInfo>> getThreadStats(String actionMasks, TaskId taskId) {
        for (Map.Entry<Tuple<String, String>, RecordingTaskManagerListener> entry : listeners.entrySet()) {
            if (actionMasks == null || entry.getKey().v2().equals(actionMasks)) {
                for (Tuple<TaskId, Map<Long, List<ThreadResourceInfo>>> threadStats : entry.getValue().getThreadStats()) {
                    if (taskId.equals(threadStats.v1())) {
                        return threadStats.v2();
                    }
                }
            }
        }
        return new HashMap<>();
    }

    /**
     * Asserts that all tasks in the tasks list have the same parentTask
     */
    protected void assertParentTask(List<TaskInfo> tasks, TaskInfo parentTask) {
        for (TaskInfo task : tasks) {
            assertParentTask(task, parentTask);
        }
    }

    protected void assertParentTask(TaskInfo task, TaskInfo parentTask) {
        assertTrue(task.getParentTaskId().isSet());
        assertEquals(parentTask.getTaskId().getNodeId(), task.getParentTaskId().getNodeId());
        assertTrue(Strings.hasLength(task.getParentTaskId().getNodeId()));
        assertEquals(parentTask.getId(), task.getParentTaskId().getId());
    }

    protected void expectNotFound(ThrowingRunnable r) {
        Exception e = expectThrows(Exception.class, r);
        ResourceNotFoundException notFound = (ResourceNotFoundException) ExceptionsHelper.unwrap(e, ResourceNotFoundException.class);
        if (notFound == null) {
            throw new AssertionError("Expected " + ResourceNotFoundException.class.getSimpleName(), e);
        }
    }

    /**
     * Fetch the task status from the list tasks API using it's "fallback to get from the task index" behavior. Asserts some obvious stuff
     * about the fetched task and returns a map of it's status.
     */
    protected GetTaskResponse expectFinishedTask(TaskId taskId) throws IOException {
        GetTaskResponse response = client().admin().cluster().prepareGetTask(taskId).get();
        assertTrue("the task should have been completed before fetching", response.getTask().isCompleted());
        TaskInfo info = response.getTask().getTask();
        assertEquals(taskId, info.getTaskId());
        assertNull(info.getStatus()); // The test task doesn't have any status
        return response;
    }

    protected void indexDocumentsWithRefresh(String indexName, int numDocs) {
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(indexName)
                .setId("test_id_" + String.valueOf(i))
                .setSource("{\"foo_" + String.valueOf(i) + "\": \"bar_" + String.valueOf(i) + "\"}", MediaTypeRegistry.JSON)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
    }
}
