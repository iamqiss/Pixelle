/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.support.clustermanager.term;

import org.density.Version;
import org.density.cluster.ClusterState;
import org.density.cluster.coordination.ClusterStateTermVersion;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Response object of cluster term
 *
 * @density.internal
 */
public class GetTermVersionResponse extends ActionResponse {

    private final ClusterStateTermVersion clusterStateTermVersion;

    private final boolean isStatePresentInRemote;

    public GetTermVersionResponse(ClusterStateTermVersion clusterStateTermVersion) {
        this.clusterStateTermVersion = clusterStateTermVersion;
        this.isStatePresentInRemote = false;
    }

    public GetTermVersionResponse(ClusterStateTermVersion clusterStateTermVersion, boolean canDownloadFromRemote) {
        this.clusterStateTermVersion = clusterStateTermVersion;
        this.isStatePresentInRemote = canDownloadFromRemote;
    }

    public GetTermVersionResponse(StreamInput in) throws IOException {
        super(in);
        this.clusterStateTermVersion = new ClusterStateTermVersion(in);
        if (in.getVersion().onOrAfter(Version.V_2_18_0)) {
            this.isStatePresentInRemote = in.readOptionalBoolean();
        } else {
            this.isStatePresentInRemote = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterStateTermVersion.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            out.writeOptionalBoolean(isStatePresentInRemote);
        }
    }

    public ClusterStateTermVersion getClusterStateTermVersion() {
        return clusterStateTermVersion;
    }

    public boolean matches(ClusterState clusterState) {
        return clusterStateTermVersion != null && clusterStateTermVersion.equals(new ClusterStateTermVersion(clusterState));
    }

    public boolean isStatePresentInRemote() {
        return isStatePresentInRemote;
    }
}
