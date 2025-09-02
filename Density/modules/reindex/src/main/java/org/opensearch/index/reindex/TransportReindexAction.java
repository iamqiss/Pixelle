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

import org.density.action.support.ActionFilters;
import org.density.action.support.AutoCreateIndex;
import org.density.action.support.HandledTransportAction;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.settings.Setting;
import org.density.common.settings.Setting.Property;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.core.action.ActionListener;
import org.density.index.reindex.spi.RemoteReindexExtension;
import org.density.script.ScriptService;
import org.density.tasks.Task;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;
import org.density.transport.client.Client;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class TransportReindexAction extends HandledTransportAction<ReindexRequest, BulkByScrollResponse> {
    public static final Setting<List<String>> REMOTE_CLUSTER_ALLOWLIST = Setting.listSetting(
        "reindex.remote.allowlist",
        emptyList(),
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<TimeValue> REMOTE_REINDEX_RETRY_INITIAL_BACKOFF = Setting.timeSetting(
        "reindex.remote.retry.initial_backoff",
        TimeValue.timeValueMillis(500),
        TimeValue.timeValueMillis(50),
        TimeValue.timeValueMillis(5000),
        Property.Dynamic,
        Property.NodeScope
    );

    public static final Setting<Integer> REMOTE_REINDEX_RETRY_MAX_COUNT = Setting.intSetting(
        "reindex.remote.retry.max_count",
        15,
        1,
        100,
        Property.Dynamic,
        Property.NodeScope
    );

    public static Optional<RemoteReindexExtension> remoteExtension = Optional.empty();

    private final ReindexValidator reindexValidator;
    private final Reindexer reindexer;

    private final ClusterService clusterService;

    @Inject
    public TransportReindexAction(
        Settings settings,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ScriptService scriptService,
        AutoCreateIndex autoCreateIndex,
        Client client,
        TransportService transportService,
        ReindexSslConfig sslConfig
    ) {
        super(ReindexAction.NAME, transportService, actionFilters, ReindexRequest::new);
        this.reindexValidator = new ReindexValidator(settings, clusterService, indexNameExpressionResolver, autoCreateIndex);
        this.reindexer = new Reindexer(clusterService, client, threadPool, scriptService, sslConfig, remoteExtension);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, ReindexRequest request, ActionListener<BulkByScrollResponse> listener) {
        if (request.getRemoteInfo() != null) {
            request.setMaxRetries(clusterService.getClusterSettings().get(REMOTE_REINDEX_RETRY_MAX_COUNT));
            request.setRetryBackoffInitialTime(clusterService.getClusterSettings().get(REMOTE_REINDEX_RETRY_INITIAL_BACKOFF));
        }

        reindexValidator.initialValidation(request);
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        reindexer.initTask(bulkByScrollTask, request, new ActionListener<Void>() {
            @Override
            public void onResponse(Void v) {
                reindexer.execute(bulkByScrollTask, request, listener);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
