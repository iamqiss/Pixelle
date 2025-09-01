/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.tasks.consumer;

import org.density.action.search.SearchShardTask;
import org.density.common.logging.DensityLogMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * Search shard task information that will be extracted from Task and converted into
 * format that will be logged
 *
 * @density.internal
 */
public final class SearchShardTaskDetailsLogMessage extends DensityLogMessage {
    SearchShardTaskDetailsLogMessage(SearchShardTask task) {
        super(prepareMap(task), message(task));
    }

    private static Map<String, Object> prepareMap(SearchShardTask task) {
        Map<String, Object> messageFields = new HashMap<>();
        messageFields.put("taskId", task.getId());
        messageFields.put("type", task.getType());
        messageFields.put("action", task.getAction());
        messageFields.put("description", task.getDescription());
        messageFields.put("start_time_millis", task.getStartTime());
        messageFields.put("parentTaskId", task.getParentTaskId());
        messageFields.put("resource_stats", task.getResourceStats());
        messageFields.put("metadata", task.getTaskMetadata());
        return messageFields;
    }

    // Message will be used in plaintext logs
    private static String message(SearchShardTask task) {
        StringBuilder sb = new StringBuilder();
        sb.append("taskId:[")
            .append(task.getId())
            .append("], ")
            .append("type:[")
            .append(task.getType())
            .append("], ")
            .append("action:[")
            .append(task.getAction())
            .append("], ")
            .append("description:[")
            .append(task.getDescription())
            .append("], ")
            .append("start_time_millis:[")
            .append(task.getStartTime())
            .append("], ")
            .append("resource_stats:[")
            .append(task.getResourceStats())
            .append("], ")
            .append("metadata:[")
            .append(task.getTaskMetadata())
            .append("]");
        return sb.toString();
    }
}
