/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.task.commons.task;

import org.density.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * Class encapsulating Task identifier
 */
@ExperimentalApi
public class TaskId {

    /**
     * Identified of the Task
     */
    private final String id;

    /**
     * Constructor to initialize TaskId
     * @param id String value of Task id
     */
    public TaskId(String id) {
        this.id = id;
    }

    /**
     * Get id value
     * @return id
     */
    public String getValue() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskId other = (TaskId) obj;
        return this.id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
