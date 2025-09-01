/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.cluster.remotestore.restore;

import org.density.action.admin.cluster.snapshots.restore.RestoreClusterStateListener;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.block.ClusterBlockLevel;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.index.recovery.RemoteStoreRestoreService;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for restore remote store operation
 *
 * @density.internal
 */
public final class TransportRestoreRemoteStoreAction extends TransportClusterManagerNodeAction<
    RestoreRemoteStoreRequest,
    RestoreRemoteStoreResponse> {
    private final RemoteStoreRestoreService restoreService;

    @Inject
    public TransportRestoreRemoteStoreAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        RemoteStoreRestoreService restoreService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            RestoreRemoteStoreAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RestoreRemoteStoreRequest::new,
            indexNameExpressionResolver
        );
        this.restoreService = restoreService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected RestoreRemoteStoreResponse read(StreamInput in) throws IOException {
        return new RestoreRemoteStoreResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(RestoreRemoteStoreRequest request, ClusterState state) {
        // Restoring a remote store might change the global state and create/change an index,
        // so we need to check for METADATA_WRITE and WRITE blocks
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException != null) {
            return blockException;
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);

    }

    @Override
    protected void clusterManagerOperation(
        final RestoreRemoteStoreRequest request,
        final ClusterState state,
        final ActionListener<RestoreRemoteStoreResponse> listener
    ) {
        restoreService.restore(request, ActionListener.delegateFailure(listener, (delegatedListener, restoreCompletionResponse) -> {
            if (restoreCompletionResponse.getRestoreInfo() == null && request.waitForCompletion()) {
                RestoreClusterStateListener.createAndRegisterListener(
                    clusterService,
                    restoreCompletionResponse,
                    delegatedListener,
                    RestoreRemoteStoreResponse::new
                );
            } else {
                delegatedListener.onResponse(new RestoreRemoteStoreResponse(restoreCompletionResponse.getRestoreInfo()));
            }
        }));
    }
}
