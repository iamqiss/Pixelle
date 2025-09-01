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

package org.density.action.ingest;

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.ingest.IngestService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Perform the action of getting a pipeline
 *
 * @density.internal
 */
public class GetPipelineTransportAction extends TransportClusterManagerNodeReadAction<GetPipelineRequest, GetPipelineResponse> {

    @Inject
    public GetPipelineTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetPipelineAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetPipelineRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetPipelineResponse read(StreamInput in) throws IOException {
        return new GetPipelineResponse(in);
    }

    @Override
    protected void clusterManagerOperation(GetPipelineRequest request, ClusterState state, ActionListener<GetPipelineResponse> listener)
        throws Exception {
        listener.onResponse(new GetPipelineResponse(IngestService.getPipelines(state, request.getIds())));
    }

    @Override
    protected ClusterBlockException checkBlock(GetPipelineRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

}
