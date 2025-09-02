/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.tasks;

import org.density.core.common.io.stream.Writeable;
import org.density.test.AbstractWireSerializingTestCase;

public class TaskCancellationStatsTests extends AbstractWireSerializingTestCase<TaskCancellationStats> {
    @Override
    protected Writeable.Reader<TaskCancellationStats> instanceReader() {
        return TaskCancellationStats::new;
    }

    @Override
    protected TaskCancellationStats createTestInstance() {
        return randomInstance();
    }

    public static TaskCancellationStats randomInstance() {
        return new TaskCancellationStats(
            SearchTaskCancellationStatsTests.randomInstance(),
            SearchShardTaskCancellationStatsTests.randomInstance()
        );
    }
}
