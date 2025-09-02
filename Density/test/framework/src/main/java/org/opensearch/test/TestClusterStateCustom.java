/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.test;

import org.density.Version;
import org.density.cluster.AbstractNamedDiffable;
import org.density.cluster.ClusterState;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class TestClusterStateCustom extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {

    private final String value;

    protected TestClusterStateCustom(String value) {
        this.value = value;
    }

    protected TestClusterStateCustom(StreamInput in) throws IOException {
        this.value = in.readString();
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

    @Override
    public boolean isPrivate() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestClusterStateCustom that = (TestClusterStateCustom) o;

        if (!value.equals(that.value)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
