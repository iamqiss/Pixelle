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

package org.density.action.main;

import org.density.Build;
import org.density.Version;
import org.density.action.support.ActionFilters;
import org.density.action.support.HandledTransportAction;
import org.density.cluster.ClusterState;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.node.Node;
import org.density.tasks.Task;
import org.density.transport.TransportService;

/**
 * Performs the main action
 *
 * @density.internal
 */
public class TransportMainAction extends HandledTransportAction<MainRequest, MainResponse> {

    private final String nodeName;
    private final ClusterService clusterService;

    @Inject
    public TransportMainAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService
    ) {
        super(MainAction.NAME, transportService, actionFilters, MainRequest::new);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, MainRequest request, ActionListener<MainResponse> listener) {
        ClusterState clusterState = clusterService.state();
        listener.onResponse(
            new MainResponse(nodeName, Version.CURRENT, clusterState.getClusterName(), clusterState.metadata().clusterUUID(), Build.CURRENT)
        );
    }
}
