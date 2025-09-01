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

package org.density.action.support.clustermanager;

import org.density.action.support.ActionFilters;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.core.action.ActionResponse;
import org.density.core.common.io.stream.Writeable;
import org.density.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

/**
 * A base class for read operations that needs to be performed on the cluster-manager node.
 * Can also be executed on the local node if needed.
 *
 * @density.internal
 */
public abstract class TransportClusterManagerNodeReadAction<
    Request extends ClusterManagerNodeReadRequest<Request>,
    Response extends ActionResponse> extends TransportClusterManagerNodeAction<Request, Response> {

    protected boolean localExecuteSupported = false;

    protected TransportClusterManagerNodeReadAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver,
        boolean localExecuteSupported
    ) {
        this(
            actionName,
            true,
            AdmissionControlActionType.CLUSTER_ADMIN,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            request,
            indexNameExpressionResolver
        );
        this.localExecuteSupported = localExecuteSupported;
    }

    protected TransportClusterManagerNodeReadAction(
        String actionName,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this(actionName, transportService, clusterService, threadPool, actionFilters, request, indexNameExpressionResolver, false);
    }

    protected TransportClusterManagerNodeReadAction(
        String actionName,
        boolean checkSizeLimit,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            actionName,
            checkSizeLimit,
            null,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            request,
            indexNameExpressionResolver
        );
    }

    protected TransportClusterManagerNodeReadAction(
        String actionName,
        boolean checkSizeLimit,
        AdmissionControlActionType admissionControlActionType,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Writeable.Reader<Request> request,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            actionName,
            checkSizeLimit,
            admissionControlActionType,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            request,
            indexNameExpressionResolver
        );
    }

    @Override
    protected final boolean localExecute(Request request) {
        return request.local();
    }

    protected boolean localExecuteSupportedByAction() {
        return localExecuteSupported;
    }

}
