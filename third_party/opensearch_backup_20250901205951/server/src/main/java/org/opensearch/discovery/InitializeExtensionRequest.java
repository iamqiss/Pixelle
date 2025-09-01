/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.discovery;

import org.density.cluster.node.DiscoveryNode;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.extensions.DiscoveryExtensionNode;
import org.density.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * InitializeExtensionRequest to intialize plugin
 *
 * @density.internal
 */
public class InitializeExtensionRequest extends TransportRequest {
    private final DiscoveryNode sourceNode;
    private final DiscoveryExtensionNode extension;
    private final String serviceAccountHeader;

    public InitializeExtensionRequest(DiscoveryNode sourceNode, DiscoveryExtensionNode extension, String serviceAccountHeader) {
        this.sourceNode = sourceNode;
        this.extension = extension;
        this.serviceAccountHeader = serviceAccountHeader;
    }

    public InitializeExtensionRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        extension = new DiscoveryExtensionNode(in);
        serviceAccountHeader = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        extension.writeTo(out);
        out.writeString(serviceAccountHeader);
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryExtensionNode getExtension() {
        return extension;
    }

    public String getServiceAccountHeader() {
        return serviceAccountHeader;
    }

    @Override
    public String toString() {
        return "InitializeExtensionsRequest{" + "sourceNode=" + sourceNode + ", extension=" + extension + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InitializeExtensionRequest that = (InitializeExtensionRequest) o;
        return Objects.equals(sourceNode, that.sourceNode)
            && Objects.equals(extension, that.extension)
            && Objects.equals(serviceAccountHeader, that.getServiceAccountHeader());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, extension);
    }
}
