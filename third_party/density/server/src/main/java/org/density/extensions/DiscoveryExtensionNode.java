/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions;

import org.density.DensityException;
import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.common.transport.TransportAddress;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Discover extensions running independently or in a separate process
 *
 * @density.internal
 */
public class DiscoveryExtensionNode extends DiscoveryNode implements Writeable, ToXContentFragment {

    private Version minimumCompatibleVersion;
    private List<ExtensionDependency> dependencies = Collections.emptyList();
    private List<String> implementedInterfaces = Collections.emptyList();

    public DiscoveryExtensionNode(
        String name,
        String id,
        TransportAddress address,
        Map<String, String> attributes,
        Version version,
        Version minimumCompatibleVersion,
        List<ExtensionDependency> dependencies
    ) {
        super(name, id, address, attributes, DiscoveryNodeRole.BUILT_IN_ROLES, version);
        this.minimumCompatibleVersion = minimumCompatibleVersion;
        this.dependencies = dependencies;
        validate();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVersion(minimumCompatibleVersion);
        out.writeVInt(dependencies.size());
        for (ExtensionDependency dependency : dependencies) {
            dependency.writeTo(out);
        }
    }

    /**
     * Construct DiscoveryExtensionNode from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurred reading the plugin info from the stream
     */
    public DiscoveryExtensionNode(final StreamInput in) throws IOException {
        super(in);
        minimumCompatibleVersion = in.readVersion();
        int size = in.readVInt();
        dependencies = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            dependencies.add(new ExtensionDependency(in));
        }
    }

    public List<ExtensionDependency> getDependencies() {
        return dependencies;
    }

    public Version getMinimumCompatibleVersion() {
        return minimumCompatibleVersion;
    }

    public List<String> getImplementedInterfaces() {
        return implementedInterfaces;
    }

    public void setImplementedInterfaces(List<String> implementedInterfaces) {
        this.implementedInterfaces = implementedInterfaces;
    }

    public boolean dependenciesContain(ExtensionDependency dependency) {
        for (ExtensionDependency extensiondependency : this.dependencies) {
            if (dependency.getUniqueId().equals(extensiondependency.getUniqueId())
                && dependency.getVersion().equals(extensiondependency.getVersion())) {
                return true;
            }
        }
        return false;
    }

    private void validate() {
        if (!Version.CURRENT.onOrAfter(minimumCompatibleVersion)) {
            throw new DensityException(
                "Extension minimumCompatibleVersion: "
                    + minimumCompatibleVersion
                    + " is greater than current Density version: "
                    + Version.CURRENT
            );
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }
}
