/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.action.admin.cluster.node.reload;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.density.ExceptionsHelper;
import org.density.DensityException;
import org.density.action.FailedNodeException;
import org.density.action.support.ActionFilters;
import org.density.action.support.nodes.TransportNodesAction;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.KeyStoreWrapper;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.settings.SecureString;
import org.density.env.Environment;
import org.density.plugins.PluginsService;
import org.density.plugins.ReloadablePlugin;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportRequest;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transport action for reloading Density Secure Settings
 *
 * @density.internal
 */
public class TransportNodesReloadSecureSettingsAction extends TransportNodesAction<
    NodesReloadSecureSettingsRequest,
    NodesReloadSecureSettingsResponse,
    TransportNodesReloadSecureSettingsAction.NodeRequest,
    NodesReloadSecureSettingsResponse.NodeResponse> {

    private final Environment environment;
    private final PluginsService pluginsService;

    @Inject
    public TransportNodesReloadSecureSettingsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Environment environment,
        PluginsService pluginService
    ) {
        super(
            NodesReloadSecureSettingsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            NodesReloadSecureSettingsRequest::new,
            NodeRequest::new,
            ThreadPool.Names.GENERIC,
            NodesReloadSecureSettingsResponse.NodeResponse.class
        );
        this.environment = environment;
        this.pluginsService = pluginService;
    }

    @Override
    protected NodesReloadSecureSettingsResponse newResponse(
        NodesReloadSecureSettingsRequest request,
        List<NodesReloadSecureSettingsResponse.NodeResponse> responses,
        List<FailedNodeException> failures
    ) {
        return new NodesReloadSecureSettingsResponse(clusterService.getClusterName(), responses, failures);
    }

    @Override
    protected NodeRequest newNodeRequest(NodesReloadSecureSettingsRequest request) {
        return new NodeRequest(request);
    }

    @Override
    protected NodesReloadSecureSettingsResponse.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodesReloadSecureSettingsResponse.NodeResponse(in);
    }

    @Override
    protected void doExecute(
        Task task,
        NodesReloadSecureSettingsRequest request,
        ActionListener<NodesReloadSecureSettingsResponse> listener
    ) {
        if (request.hasPassword() && isNodeLocal(request) == false && isNodeTransportTLSEnabled() == false) {
            request.closePassword();
            listener.onFailure(
                new DensityException(
                    "Secure settings cannot be updated cluster wide when TLS for the transport layer"
                        + " is not enabled. Enable TLS or use the API with a `_local` filter on each node."
                )
            );
        } else {
            super.doExecute(task, request, ActionListener.wrap(response -> {
                request.closePassword();
                listener.onResponse(response);
            }, e -> {
                request.closePassword();
                listener.onFailure(e);
            }));
        }
    }

    @Override
    protected NodesReloadSecureSettingsResponse.NodeResponse nodeOperation(NodeRequest nodeReloadRequest) {
        final NodesReloadSecureSettingsRequest request = nodeReloadRequest.request;
        // We default to using an empty string as the keystore password so that we mimic pre 7.3 API behavior
        final SecureString secureSettingsPassword = request.hasPassword()
            ? request.getSecureSettingsPassword()
            : new SecureString(new char[0]);
        try (KeyStoreWrapper keystore = KeyStoreWrapper.load(environment.configDir())) {
            // reread keystore from config file
            if (keystore == null) {
                return new NodesReloadSecureSettingsResponse.NodeResponse(
                    clusterService.localNode(),
                    new IllegalStateException("Keystore is missing")
                );
            }
            // decrypt the keystore using the password from the request
            keystore.decrypt(secureSettingsPassword.getChars());
            // add the keystore to the original node settings object
            final Settings settingsWithKeystore = Settings.builder().put(environment.settings(), false).setSecureSettings(keystore).build();
            final List<Exception> exceptions = new ArrayList<>();
            // broadcast the new settings object (with the open embedded keystore) to all reloadable plugins
            pluginsService.filterPlugins(ReloadablePlugin.class).stream().forEach(p -> {
                try {
                    p.reload(settingsWithKeystore);
                } catch (final Exception e) {
                    logger.warn(
                        (Supplier<?>) () -> new ParameterizedMessage("Reload failed for plugin [{}]", p.getClass().getSimpleName()),
                        e
                    );
                    exceptions.add(e);
                }
            });
            ExceptionsHelper.rethrowAndSuppress(exceptions);
            return new NodesReloadSecureSettingsResponse.NodeResponse(clusterService.localNode(), null);
        } catch (final Exception e) {
            return new NodesReloadSecureSettingsResponse.NodeResponse(clusterService.localNode(), e);
        } finally {
            secureSettingsPassword.close();
        }
    }

    /**
     * Inner Node Request
     *
     * @density.internal
     */
    public static class NodeRequest extends TransportRequest {

        NodesReloadSecureSettingsRequest request;

        public NodeRequest(StreamInput in) throws IOException {
            super(in);
            request = new NodesReloadSecureSettingsRequest(in);
        }

        NodeRequest(NodesReloadSecureSettingsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }

    /**
     * Returns true if the node is configured for TLS on the transport layer
     */
    private boolean isNodeTransportTLSEnabled() {
        return transportService.isTransportSecure();
    }

    private boolean isNodeLocal(NodesReloadSecureSettingsRequest request) {
        if (null == request.concreteNodes()) {
            resolveRequest(request, clusterService.state());
            assert request.concreteNodes() != null;
        }
        final DiscoveryNode[] nodes = request.concreteNodes();
        return nodes.length == 1 && nodes[0].getId().equals(clusterService.localNode().getId());
    }
}
