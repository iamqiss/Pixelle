/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.search.aggregations.metrics;

import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents a scripted average calculation containing a sum and count.
 *
 * @density.internal
 */
public class ScriptedAvg implements Writeable {
    private double sum;
    private long count;

    /**
     * Constructor for ScriptedAvg
     *
     * @param sum   The sum of values
     * @param count The count of values
     */
    public ScriptedAvg(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    /**
     * Read from a stream.
     */
    public ScriptedAvg(StreamInput in) throws IOException {
        this.sum = in.readDouble();
        this.count = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    public double getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

}
