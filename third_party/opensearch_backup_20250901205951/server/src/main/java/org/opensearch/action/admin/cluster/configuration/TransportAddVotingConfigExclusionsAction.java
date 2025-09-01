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

package org.density.action.admin.cluster.configuration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.DensityException;
import org.density.DensityTimeoutException;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.ClusterStateObserver;
import org.density.cluster.ClusterStateObserver.Listener;
import org.density.cluster.ClusterStateUpdateTask;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.Priority;
import org.density.common.inject.Inject;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.threadpool.ThreadPool.Names;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.density.action.admin.cluster.configuration.VotingConfigExclusionsHelper.addExclusionAndGetState;
import static org.density.action.admin.cluster.configuration.VotingConfigExclusionsHelper.resolveVotingConfigExclusionsAndCheckMaximum;

/**
 * Transport endpoint action for adding exclusions to voting config
 *
 * @density.internal
 */
public class TransportAddVotingConfigExclusionsAction extends TransportClusterManagerNodeAction<
    AddVotingConfigExclusionsRequest,
    AddVotingConfigExclusionsResponse> {

    private static final Logger logger = LogManager.getLogger(TransportAddVotingConfigExclusionsAction.class);

    public static final Setting<Integer> MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING = Setting.intSetting(
        "cluster.max_voting_config_exclusions",
        10,
        1,
        Property.Dynamic,
        Property.NodeScope
    );

    private volatile int maxVotingConfigExclusions;

    @Inject
    public TransportAddVotingConfigExclusionsAction(
        Settings settings,
        ClusterSettings clusterSettings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            AddVotingConfigExclusionsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            AddVotingConfigExclusionsRequest::new,
            indexNameExpressionResolver
        );

        maxVotingConfigExclusions = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING, this::setMaxVotingConfigExclusions);
    }

    private void setMaxVotingConfigExclusions(int maxVotingConfigExclusions) {
        this.maxVotingConfigExclusions = maxVotingConfigExclusions;
    }

    @Override
    protected String executor() {
        return Names.SAME;
    }

    @Override
    protected AddVotingConfigExclusionsResponse read(StreamInput in) throws IOException {
        return new AddVotingConfigExclusionsResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        AddVotingConfigExclusionsRequest request,
        ClusterState state,
        ActionListener<AddVotingConfigExclusionsResponse> listener
    ) throws Exception {

        resolveVotingConfigExclusionsAndCheckMaximum(request, state, maxVotingConfigExclusions);
        // throws IAE if no nodes matched or maximum exceeded

        clusterService.submitStateUpdateTask("add-voting-config-exclusions", new ClusterStateUpdateTask(Priority.URGENT) {

            private Set<VotingConfigExclusion> resolvedExclusions;

            @Override
            public ClusterState execute(ClusterState currentState) {
                assert resolvedExclusions == null : resolvedExclusions;
                final int finalMaxVotingConfigExclusions = TransportAddVotingConfigExclusionsAction.this.maxVotingConfigExclusions;
                resolvedExclusions = resolveVotingConfigExclusionsAndCheckMaximum(request, currentState, finalMaxVotingConfigExclusions);
                return addExclusionAndGetState(currentState, resolvedExclusions, finalMaxVotingConfigExclusions);
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {

                final ClusterStateObserver observer = new ClusterStateObserver(
                    clusterService,
                    request.getTimeout(),
                    logger,
                    threadPool.getThreadContext()
                );

                final Set<String> excludedNodeIds = resolvedExclusions.stream()
                    .map(VotingConfigExclusion::getNodeId)
                    .collect(Collectors.toSet());

                final Predicate<ClusterState> allNodesRemoved = clusterState -> {
                    final Set<String> votingConfigNodeIds = clusterState.getLastCommittedConfiguration().getNodeIds();
                    return excludedNodeIds.stream().noneMatch(votingConfigNodeIds::contains);
                };

                final Listener clusterStateListener = new Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(new AddVotingConfigExclusionsResponse());
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(
                            new DensityException(
                                "cluster service closed while waiting for voting config exclusions "
                                    + resolvedExclusions
                                    + " to take effect"
                            )
                        );
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(
                            new DensityTimeoutException(
                                "timed out waiting for voting config exclusions " + resolvedExclusions + " to take effect"
                            )
                        );
                    }
                };

                if (allNodesRemoved.test(newState)) {
                    clusterStateListener.onNewClusterState(newState);
                } else {
                    observer.waitForNextChange(clusterStateListener, allNodesRemoved);
                }
            }
        });
    }

    @Override
    protected ClusterBlockException checkBlock(AddVotingConfigExclusionsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
