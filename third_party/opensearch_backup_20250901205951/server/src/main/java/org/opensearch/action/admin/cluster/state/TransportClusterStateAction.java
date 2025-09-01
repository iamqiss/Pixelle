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

package org.density.action.admin.cluster.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.density.cluster.ClusterState;
import org.density.cluster.ClusterStateObserver;
import org.density.cluster.NotClusterManagerException;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.Metadata;
import org.density.cluster.metadata.Metadata.Custom;
import org.density.cluster.routing.RoutingTable;
import org.density.cluster.service.ClusterService;
import org.density.common.Nullable;
import org.density.common.inject.Inject;
import org.density.common.unit.TimeValue;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.gateway.remote.RemoteClusterStateService;
import org.density.node.NodeClosedException;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Transport action for obtaining cluster state
 *
 * @density.internal
 */
public class TransportClusterStateAction extends TransportClusterManagerNodeReadAction<ClusterStateRequest, ClusterStateResponse> {

    private final Logger logger = LogManager.getLogger(getClass());

    static {
        final String property = System.getProperty("density.cluster_state.size");
        if (property != null) {
            throw new IllegalArgumentException("density.cluster_state.size is no longer respected but was [" + property + "]");
        }
    }

    @Inject
    public TransportClusterStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        @Nullable RemoteClusterStateService remoteClusterStateService
    ) {
        super(
            ClusterStateAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterStateRequest::new,
            indexNameExpressionResolver
        );
        this.localExecuteSupported = true;
        this.remoteClusterStateService = remoteClusterStateService;
    }

    @Override
    protected String executor() {
        // very lightweight operation in memory, no need to fork to a thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterStateResponse read(StreamInput in) throws IOException {
        return new ClusterStateResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterStateRequest request, ClusterState state) {
        // cluster state calls are done also on a fully blocked cluster to figure out what is going
        // on in the cluster. For example, which nodes have joined yet the recovery has not yet kicked
        // in, we need to make sure we allow those calls
        // return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        final ClusterStateRequest request,
        final ClusterState state,
        final ActionListener<ClusterStateResponse> listener
    ) throws IOException {

        final Predicate<ClusterState> acceptableClusterStatePredicate = request.waitForMetadataVersion() == null
            ? clusterState -> true
            : clusterState -> clusterState.metadata().version() >= request.waitForMetadataVersion();

        // action will be executed on local node, if either the request is local only (or) the local node has the same cluster-state as
        // ClusterManager
        final Predicate<ClusterState> acceptableClusterStateOrNotMasterPredicate = request.local()
            || !state.nodes().isLocalNodeElectedClusterManager()
                ? acceptableClusterStatePredicate
                : acceptableClusterStatePredicate.or(clusterState -> clusterState.nodes().isLocalNodeElectedClusterManager() == false);

        if (acceptableClusterStatePredicate.test(state)) {
            ActionListener.completeWith(listener, () -> buildResponse(request, state));
        } else {
            assert acceptableClusterStateOrNotMasterPredicate.test(state) == false;
            new ClusterStateObserver(state, clusterService, request.waitForTimeout(), logger, threadPool.getThreadContext())
                .waitForNextChange(new ClusterStateObserver.Listener() {

                    @Override
                    public void onNewClusterState(ClusterState newState) {
                        if (acceptableClusterStatePredicate.test(newState)) {
                            ActionListener.completeWith(listener, () -> buildResponse(request, newState));
                        } else {
                            listener.onFailure(
                                new NotClusterManagerException(
                                    "cluster-manager stepped down waiting for metadata version " + request.waitForMetadataVersion()
                                )
                            );
                        }
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        try {
                            listener.onResponse(new ClusterStateResponse(state.getClusterName(), null, true));
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }
                }, acceptableClusterStateOrNotMasterPredicate);
        }
    }

    private ClusterStateResponse buildResponse(final ClusterStateRequest request, final ClusterState currentState) {
        logger.trace("Serving cluster state request using version {}", currentState.version());
        ClusterState.Builder builder = ClusterState.builder(currentState.getClusterName());
        builder.version(currentState.version());
        builder.stateUUID(currentState.stateUUID());

        if (request.nodes()) {
            builder.nodes(currentState.nodes());
        }
        if (request.routingTable()) {
            if (request.indices().length > 0) {
                RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    if (currentState.routingTable().getIndicesRouting().containsKey(filteredIndex)) {
                        routingTableBuilder.add(currentState.routingTable().getIndicesRouting().get(filteredIndex));
                    }
                }
                builder.routingTable(routingTableBuilder.build());
            } else {
                builder.routingTable(currentState.routingTable());
            }
        }
        if (request.blocks()) {
            builder.blocks(currentState.blocks());
        }

        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.clusterUUID(currentState.metadata().clusterUUID());
        mdBuilder.coordinationMetadata(currentState.coordinationMetadata());

        if (request.metadata()) {
            if (request.indices().length > 0) {
                mdBuilder.version(currentState.metadata().version());
                String[] indices = indexNameExpressionResolver.concreteIndexNames(currentState, request);
                for (String filteredIndex : indices) {
                    IndexMetadata indexMetadata = currentState.metadata().index(filteredIndex);
                    if (indexMetadata != null) {
                        mdBuilder.put(indexMetadata, false);
                    }
                }
            } else {
                mdBuilder = Metadata.builder(currentState.metadata());
            }

            // filter out metadata that shouldn't be returned by the API
            for (final Map.Entry<String, Custom> custom : currentState.metadata().customs().entrySet()) {
                if (custom.getValue().context().contains(Metadata.XContentContext.API) == false) {
                    mdBuilder.removeCustom(custom.getKey());
                }
            }
        }
        builder.metadata(mdBuilder);

        if (request.customs()) {
            for (final Map.Entry<String, ClusterState.Custom> custom : currentState.customs().entrySet()) {
                if (custom.getValue().isPrivate() == false) {
                    builder.putCustom(custom.getKey(), custom.getValue());
                }
            }
        }

        return new ClusterStateResponse(currentState.getClusterName(), builder.build(), false);
    }
}
