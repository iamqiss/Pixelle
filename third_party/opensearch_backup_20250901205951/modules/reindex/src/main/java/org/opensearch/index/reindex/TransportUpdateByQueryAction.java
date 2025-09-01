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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.index.reindex;

import org.apache.logging.log4j.Logger;
import org.density.action.index.IndexRequest;
import org.density.action.support.ActionFilters;
import org.density.action.support.HandledTransportAction;
import org.density.cluster.ClusterState;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.Writeable;
import org.density.index.mapper.IdFieldMapper;
import org.density.index.mapper.IndexFieldMapper;
import org.density.index.mapper.RoutingFieldMapper;
import org.density.script.Script;
import org.density.script.ScriptService;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.client.Client;
import org.density.transport.client.ParentTaskAssigningClient;

import java.util.Map;
import java.util.function.BiFunction;

public class TransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkByScrollResponse> {

    private final ThreadPool threadPool;
    private final Client client;
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    @Inject
    public TransportUpdateByQueryAction(
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        ClusterService clusterService
    ) {
        super(
            UpdateByQueryAction.NAME,
            transportService,
            actionFilters,
            (Writeable.Reader<UpdateByQueryRequest>) UpdateByQueryRequest::new
        );
        this.threadPool = threadPool;
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, UpdateByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        BulkByScrollParallelizationHelper.startSlicedAction(
            request,
            bulkByScrollTask,
            UpdateByQueryAction.INSTANCE,
            listener,
            client,
            clusterService.localNode(),
            () -> {
                ClusterState state = clusterService.state();
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(
                    client,
                    clusterService.localNode(),
                    bulkByScrollTask
                );
                new AsyncIndexBySearchAction(bulkByScrollTask, logger, assigningClient, threadPool, scriptService, request, state, listener)
                    .start();
            }
        );
    }

    /**
     * Simple implementation of update-by-query using scrolling and bulk.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkByScrollAction<UpdateByQueryRequest, TransportUpdateByQueryAction> {

        AsyncIndexBySearchAction(
            BulkByScrollTask task,
            Logger logger,
            ParentTaskAssigningClient client,
            ThreadPool threadPool,
            ScriptService scriptService,
            UpdateByQueryRequest request,
            ClusterState clusterState,
            ActionListener<BulkByScrollResponse> listener
        ) {
            super(task, false, true, logger, client, threadPool, request, listener, scriptService, null);
        }

        @Override
        public BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                return new UpdateByQueryScriptApplier(worker, scriptService, script, script.getParams());
            }
            return super.buildScriptApplier();
        }

        @Override
        protected org.density.action.bulk.BulkRequest buildBulkRequest() {
            return new org.density.action.bulk.BulkRequest().pipeline(mainRequest.getPipeline());
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(ScrollableHitSource.Hit doc) {
            IndexRequest index = new IndexRequest();
            index.index(doc.getIndex());
            index.id(doc.getId());
            index.source(doc.getSource(), doc.getMediaType());
            index.setIfSeqNo(doc.getSeqNo());
            index.setIfPrimaryTerm(doc.getPrimaryTerm());
            index.setPipeline(mainRequest.getPipeline());
            return wrap(index);
        }

        class UpdateByQueryScriptApplier extends ScriptApplier {

            UpdateByQueryScriptApplier(
                WorkerBulkByScrollTaskState taskWorker,
                ScriptService scriptService,
                Script script,
                Map<String, Object> params
            ) {
                super(taskWorker, scriptService, script, params);
            }

            @Override
            protected void scriptChangedIndex(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + IndexFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedId(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + IdFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedVersion(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [_version] not allowed");
            }

            @Override
            protected void scriptChangedRouting(RequestWrapper<?> request, Object to) {
                throw new IllegalArgumentException("Modifying [" + RoutingFieldMapper.NAME + "] not allowed");
            }

        }
    }
}
