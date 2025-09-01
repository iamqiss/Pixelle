/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.tasks;

import org.density.Version;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Holds stats related to task cancellation.
 */
public class TaskCancellationStats implements ToXContentFragment, Writeable {

    private final SearchTaskCancellationStats searchTaskCancellationStats;
    private final SearchShardTaskCancellationStats searchShardTaskCancellationStats;

    public TaskCancellationStats(
        SearchTaskCancellationStats searchTaskCancellationStats,
        SearchShardTaskCancellationStats searchShardTaskCancellationStats
    ) {
        this.searchTaskCancellationStats = searchTaskCancellationStats;
        this.searchShardTaskCancellationStats = searchShardTaskCancellationStats;
    }

    public TaskCancellationStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            searchTaskCancellationStats = new SearchTaskCancellationStats(in);
        } else {
            searchTaskCancellationStats = new SearchTaskCancellationStats(0, 0);
        }
        searchShardTaskCancellationStats = new SearchShardTaskCancellationStats(in);
    }

    // package private for testing
    protected SearchShardTaskCancellationStats getSearchShardTaskCancellationStats() {
        return this.searchShardTaskCancellationStats;
    }

    // package private for testing
    protected SearchTaskCancellationStats getSearchTaskCancellationStats() {
        return this.searchTaskCancellationStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("task_cancellation");
        builder.field("search_task", searchTaskCancellationStats);
        builder.field("search_shard_task", searchShardTaskCancellationStats);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            searchTaskCancellationStats.writeTo(out);
        }
        searchShardTaskCancellationStats.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TaskCancellationStats that = (TaskCancellationStats) o;
        return Objects.equals(searchTaskCancellationStats, that.searchTaskCancellationStats)
            && Objects.equals(searchShardTaskCancellationStats, that.searchShardTaskCancellationStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchTaskCancellationStats, searchShardTaskCancellationStats);
    }
}
