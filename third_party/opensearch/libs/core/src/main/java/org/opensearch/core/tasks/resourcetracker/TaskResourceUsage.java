/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.core.tasks.resourcetracker;

import org.density.common.annotation.PublicApi;
import org.density.core.ParseField;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.ConstructingObjectParser;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.density.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Task resource usage information
 * <p>
 * Writeable TaskResourceUsage objects are used to represent resource usage
 * information of running tasks.
 *
 *  @density.api
 */
@PublicApi(since = "2.1.0")
public class TaskResourceUsage implements Writeable, ToXContentFragment {
    private static final ParseField CPU_TIME_IN_NANOS = new ParseField("cpu_time_in_nanos");
    private static final ParseField MEMORY_IN_BYTES = new ParseField("memory_in_bytes");

    private final long cpuTimeInNanos;
    private final long memoryInBytes;

    public TaskResourceUsage(long cpuTimeInNanos, long memoryInBytes) {
        this.cpuTimeInNanos = cpuTimeInNanos;
        this.memoryInBytes = memoryInBytes;
    }

    /**
     * Read from a stream.
     */
    public static TaskResourceUsage readFromStream(StreamInput in) throws IOException {
        return new TaskResourceUsage(in.readVLong(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(cpuTimeInNanos);
        out.writeVLong(memoryInBytes);
    }

    public long getCpuTimeInNanos() {
        return cpuTimeInNanos;
    }

    public long getMemoryInBytes() {
        return memoryInBytes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(CPU_TIME_IN_NANOS.getPreferredName(), cpuTimeInNanos);
        builder.field(MEMORY_IN_BYTES.getPreferredName(), memoryInBytes);
        return builder;
    }

    public static final ConstructingObjectParser<TaskResourceUsage, Void> PARSER = new ConstructingObjectParser<>(
        "task_resource_usage",
        a -> new TaskResourceUsage((Long) a[0], (Long) a[1])
    );

    static {
        PARSER.declareLong(constructorArg(), CPU_TIME_IN_NANOS);
        PARSER.declareLong(constructorArg(), MEMORY_IN_BYTES);
    }

    public static TaskResourceUsage fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskResourceUsage.class) {
            return false;
        }
        TaskResourceUsage other = (TaskResourceUsage) obj;
        return Objects.equals(cpuTimeInNanos, other.cpuTimeInNanos) && Objects.equals(memoryInBytes, other.memoryInBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpuTimeInNanos, memoryInBytes);
    }
}
