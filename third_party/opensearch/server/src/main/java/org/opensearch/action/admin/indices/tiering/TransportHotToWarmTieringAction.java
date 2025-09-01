/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.tiering;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterInfoService;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.routing.allocation.DiskThresholdSettings;
import org.density.cluster.service.ClusterService;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.inject.Inject;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.index.Index;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.Set;

import static org.density.indices.tiering.TieringRequestValidator.validateHotToWarm;

/**
 * Transport Tiering action to move indices from hot to warm
 *
 * @density.experimental
 */
@ExperimentalApi
public class TransportHotToWarmTieringAction extends TransportClusterManagerNodeAction<TieringIndexRequest, HotToWarmTieringResponse> {

    private static final Logger logger = LogManager.getLogger(TransportHotToWarmTieringAction.class);
    private final ClusterInfoService clusterInfoService;
    private final DiskThresholdSettings diskThresholdSettings;

    @Inject
    public TransportHotToWarmTieringAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterInfoService clusterInfoService,
        Settings settings
    ) {
        super(
            HotToWarmTieringAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            TieringIndexRequest::new,
            indexNameExpressionResolver
        );
        this.clusterInfoService = clusterInfoService;
        this.diskThresholdSettings = new DiskThresholdSettings(settings, clusterService.getClusterSettings());
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected HotToWarmTieringResponse read(StreamInput in) throws IOException {
        return new HotToWarmTieringResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(TieringIndexRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        TieringIndexRequest request,
        ClusterState state,
        ActionListener<HotToWarmTieringResponse> listener
    ) throws Exception {
        Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new HotToWarmTieringResponse(true));
            return;
        }
        final TieringValidationResult tieringValidationResult = validateHotToWarm(
            state,
            Set.of(concreteIndices),
            clusterInfoService.getClusterInfo(),
            diskThresholdSettings
        );

        if (tieringValidationResult.getAcceptedIndices().isEmpty()) {
            listener.onResponse(tieringValidationResult.constructResponse());
            return;
        }
    }
}
