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

package org.density.action.admin.cluster.storedscripts;

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterManagerTaskThrottler;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.script.ScriptService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

import static org.density.cluster.service.ClusterManagerTask.DELETE_SCRIPT;

/**
 * Transport action for deleting stored script
 *
 * @density.internal
 */
public class TransportDeleteStoredScriptAction extends TransportClusterManagerNodeAction<DeleteStoredScriptRequest, AcknowledgedResponse> {

    private final ScriptService scriptService;
    private final ClusterManagerTaskThrottler.ThrottlingKey deleteScriptTaskKey;

    @Inject
    public TransportDeleteStoredScriptAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ScriptService scriptService
    ) {
        super(
            DeleteStoredScriptAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteStoredScriptRequest::new,
            indexNameExpressionResolver
        );
        this.scriptService = scriptService;
        // Task is onboarded for throttling, it will get retried from associated TransportClusterManagerNodeAction.
        deleteScriptTaskKey = clusterService.registerClusterManagerTask(DELETE_SCRIPT, true);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        DeleteStoredScriptRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        scriptService.deleteStoredScript(clusterService, request, deleteScriptTaskKey, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteStoredScriptRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}
