/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.support.clustermanager.term;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.support.ActionFilters;
import org.density.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.density.cluster.ClusterState;
import org.density.cluster.block.ClusterBlockException;
import org.density.cluster.coordination.ClusterStateTermVersion;
import org.density.cluster.coordination.Coordinator;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.common.util.FeatureFlags;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.StreamInput;
import org.density.discovery.Discovery;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for obtaining cluster term and version from cluster-manager
 *
 * @density.internal
 */
public class TransportGetTermVersionAction extends TransportClusterManagerNodeReadAction<GetTermVersionRequest, GetTermVersionResponse> {

    private final Logger logger = LogManager.getLogger(getClass());

    private final Discovery discovery;

    private boolean usePreCommitState = false;

    @Inject
    public TransportGetTermVersionAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Discovery discovery
    ) {
        super(
            GetTermVersionAction.NAME,
            false,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetTermVersionRequest::new,
            indexNameExpressionResolver
        );
        this.usePreCommitState = FeatureFlags.isEnabled(FeatureFlags.TERM_VERSION_PRECOMMIT_ENABLE_SETTING);
        this.discovery = discovery;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    public GetTermVersionResponse read(StreamInput in) throws IOException {
        return new GetTermVersionResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(GetTermVersionRequest request, ClusterState state) {
        // cluster state term and version needs to be retrieved even on a fully blocked cluster
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        GetTermVersionRequest request,
        ClusterState state,
        ActionListener<GetTermVersionResponse> listener
    ) throws Exception {
        if (usePreCommitState) {
            ActionListener.completeWith(listener, () -> buildResponse(request, clusterService.preCommitState()));
        } else {
            ActionListener.completeWith(listener, () -> buildResponse(request, state));
        }

    }

    private GetTermVersionResponse buildResponse(GetTermVersionRequest request, ClusterState state) {
        ClusterStateTermVersion termVersion = new ClusterStateTermVersion(state);
        if (discovery instanceof Coordinator) {
            Coordinator coordinator = (Coordinator) discovery;
            if (coordinator.canDownloadFullStateFromRemote()) {
                return new GetTermVersionResponse(termVersion, coordinator.isRemotePublicationEnabled());
            }
        }
        return new GetTermVersionResponse(termVersion);
    }
}
