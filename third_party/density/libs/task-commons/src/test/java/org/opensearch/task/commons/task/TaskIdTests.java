/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.task.commons.task;

import org.density.test.DensityTestCase;

/**
 * Tests for {@link TaskId}
 */
public class TaskIdTests extends DensityTestCase {

    public void testConstructorAndGetValue() {
        TaskId taskId = new TaskId("123");
        assertEquals("123", taskId.getValue());
    }

    public void testEqualsWithSameId() {
        TaskId taskId1 = new TaskId("456");
        TaskId taskId2 = new TaskId("456");
        assertEquals(taskId1, taskId2);
    }

    public void testEqualsWithDifferentId() {
        TaskId taskId1 = new TaskId("789");
        TaskId taskId2 = new TaskId("987");
        assertNotEquals(taskId1, taskId2);
    }

    public void testEqualsWithNull() {
        TaskId taskId = new TaskId("abc");
        assertNotEquals(null, taskId);
    }

    public void testEqualsWithDifferentClass() {
        TaskId taskId = new TaskId("def");
        assertNotEquals(taskId, new Object());
    }

    public void testHashCode() {
        TaskId taskId1 = new TaskId("456");
        TaskId taskId2 = new TaskId("456");
        assertEquals(taskId1.hashCode(), taskId2.hashCode());

        TaskId taskId3 = new TaskId("4567");
        assertNotEquals(taskId1.hashCode(), taskId3.hashCode());
    }
}
