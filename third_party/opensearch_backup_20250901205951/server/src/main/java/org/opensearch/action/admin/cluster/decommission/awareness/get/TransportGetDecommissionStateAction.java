/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.decommission.awareness.get;

import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.decommission.DecommissionAttributeMetadata;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for getting decommission status
 *
 * @density.internal
 */
public class TransportGetDecommissionStateAction extends TransportClusterManagerNodeReadAction<
    GetDecommissionStateRequest,
    GetDecommissionStateResponse> {

    @Inject
    public TransportGetDecommissionStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            GetDecommissionStateAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetDecommissionStateRequest::new,
            indexNameExpressionResolver,
            true
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetDecommissionStateResponse read(StreamInput in) throws IOException {
        return new GetDecommissionStateResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        GetDecommissionStateRequest request,
        ClusterState state,
        ActionListener<GetDecommissionStateResponse> listener
    ) throws Exception {
        DecommissionAttributeMetadata decommissionAttributeMetadata = state.metadata().decommissionAttributeMetadata();
        if (decommissionAttributeMetadata != null
            && request.attributeName().equals(decommissionAttributeMetadata.decommissionAttribute().attributeName())) {
            listener.onResponse(
                new GetDecommissionStateResponse(
                    decommissionAttributeMetadata.decommissionAttribute().attributeValue(),
                    decommissionAttributeMetadata.status()
                )
            );
        } else {
            listener.onResponse(new GetDecommissionStateResponse());
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetDecommissionStateRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
