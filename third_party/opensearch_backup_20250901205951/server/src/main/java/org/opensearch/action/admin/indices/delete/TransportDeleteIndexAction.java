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

package org.density.action.admin.indices.delete;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.action.support.ActionFilters;
import org.density.action.support.DestructiveOperations;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.ack.ClusterStateUpdateResponse;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.metadata.MetadataDeleteIndexService;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.index.Index;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Delete index action.
 *
 * @density.internal
 */
public class TransportDeleteIndexAction extends TransportClusterManagerNodeAction<DeleteIndexRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteIndexAction.class);

    private final MetadataDeleteIndexService deleteIndexService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportDeleteIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataDeleteIndexService deleteIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            DeleteIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteIndexRequest::new,
            indexNameExpressionResolver
        );
        this.deleteIndexService = deleteIndexService;
        this.destructiveOperations = destructiveOperations;
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
    protected void doExecute(Task task, DeleteIndexRequest request, ActionListener<AcknowledgedResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteIndexRequest request, ClusterState state) {
        return state.blocks().indicesAllowReleaseResources(indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final DeleteIndexRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        final Set<Index> concreteIndices = new HashSet<>(Arrays.asList(indexNameExpressionResolver.concreteIndices(state, request)));
        if (concreteIndices.isEmpty()) {
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }

        DeleteIndexClusterStateUpdateRequest deleteRequest = new DeleteIndexClusterStateUpdateRequest().ackTimeout(request.timeout())
            .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())
            .indices(concreteIndices.toArray(new Index[0]));

        deleteIndexService.deleteIndices(deleteRequest, new ActionListener<ClusterStateUpdateResponse>() {

            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(() -> new ParameterizedMessage("failed to delete indices [{}]", concreteIndices), t);
                listener.onFailure(t);
            }
        });
    }
}
