/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.action.StepListener;
import org.density.action.support.GroupedActionListener;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.service.ClusterService;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.Strings;
import org.density.core.common.io.stream.StreamInput;
import org.density.threadpool.ThreadPool;
import org.density.transport.Transport;
import org.density.transport.TransportException;
import org.density.transport.TransportResponseHandler;
import org.density.transport.TransportService;
import org.density.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Service class for PIT reusable functions
 */
public class PitService {

    private static final Logger logger = LogManager.getLogger(PitService.class);

    private final ClusterService clusterService;
    private final SearchTransportService searchTransportService;
    private final TransportService transportService;
    private final NodeClient nodeClient;

    @Inject
    public PitService(
        ClusterService clusterService,
        SearchTransportService searchTransportService,
        TransportService transportService,
        NodeClient nodeClient
    ) {
        this.clusterService = clusterService;
        this.searchTransportService = searchTransportService;
        this.transportService = transportService;
        this.nodeClient = nodeClient;
    }

    /**
     * Delete list of pit contexts. Returns the details of success of operation per PIT ID.
     */
    public void deletePitContexts(
        Map<String, List<PitSearchContextIdForNode>> nodeToContextsMap,
        ActionListener<DeletePitResponse> listener
    ) {
        if (nodeToContextsMap.size() == 0) {
            listener.onResponse(new DeletePitResponse(Collections.emptyList()));
            return;
        }
        final Set<String> clusters = nodeToContextsMap.values()
            .stream()
            .flatMap(Collection::stream)
            .filter(ctx -> Strings.isEmpty(ctx.getSearchContextIdForNode().getClusterAlias()) == false)
            .map(c -> c.getSearchContextIdForNode().getClusterAlias())
            .collect(Collectors.toSet());
        StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = (StepListener<
            BiFunction<String, String, DiscoveryNode>>) SearchUtils.getConnectionLookupListener(
                searchTransportService.getRemoteClusterService(),
                clusterService.state(),
                clusters
            );
        lookupListener.whenComplete(nodeLookup -> {
            final GroupedActionListener<DeletePitResponse> groupedListener = getDeletePitGroupedListener(
                listener,
                nodeToContextsMap.size()
            );

            for (Map.Entry<String, List<PitSearchContextIdForNode>> entry : nodeToContextsMap.entrySet()) {
                String clusterAlias = entry.getValue().get(0).getSearchContextIdForNode().getClusterAlias();
                DiscoveryNode node = nodeLookup.apply(clusterAlias, entry.getValue().get(0).getSearchContextIdForNode().getNode());
                if (node == null) {
                    node = this.clusterService.state().getNodes().get(entry.getValue().get(0).getSearchContextIdForNode().getNode());
                }
                if (node == null) {
                    logger.error(
                        () -> new ParameterizedMessage("node [{}] not found", entry.getValue().get(0).getSearchContextIdForNode().getNode())
                    );
                    List<DeletePitInfo> deletePitInfos = new ArrayList<>();
                    for (PitSearchContextIdForNode pitSearchContextIdForNode : entry.getValue()) {
                        deletePitInfos.add(new DeletePitInfo(false, pitSearchContextIdForNode.getPitId()));
                    }
                    groupedListener.onResponse(new DeletePitResponse(deletePitInfos));
                } else {
                    try {
                        final Transport.Connection connection = searchTransportService.getConnection(clusterAlias, node);
                        searchTransportService.sendFreePITContexts(connection, entry.getValue(), groupedListener);
                    } catch (Exception e) {
                        String nodeName = node.getName();
                        logger.error(() -> new ParameterizedMessage("Delete PITs failed on node [{}]", nodeName), e);
                        List<DeletePitInfo> deletePitInfos = new ArrayList<>();
                        for (PitSearchContextIdForNode pitSearchContextIdForNode : entry.getValue()) {
                            deletePitInfos.add(new DeletePitInfo(false, pitSearchContextIdForNode.getPitId()));
                        }
                        groupedListener.onResponse(new DeletePitResponse(deletePitInfos));
                    }
                }
            }
        }, listener::onFailure);
    }

    public GroupedActionListener<DeletePitResponse> getDeletePitGroupedListener(ActionListener<DeletePitResponse> listener, int size) {
        return new GroupedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(final Collection<DeletePitResponse> responses) {
                Map<String, Boolean> pitIdToSucceededMap = new HashMap<>();
                for (DeletePitResponse response : responses) {
                    for (DeletePitInfo deletePitInfo : response.getDeletePitResults()) {
                        if (!pitIdToSucceededMap.containsKey(deletePitInfo.getPitId())) {
                            pitIdToSucceededMap.put(deletePitInfo.getPitId(), deletePitInfo.isSuccessful());
                        }
                        if (!deletePitInfo.isSuccessful()) {
                            logger.debug(() -> new ParameterizedMessage("Deleting PIT with ID {} failed ", deletePitInfo.getPitId()));
                            pitIdToSucceededMap.put(deletePitInfo.getPitId(), deletePitInfo.isSuccessful());
                        }
                    }
                }
                List<DeletePitInfo> deletePitResults = new ArrayList<>();
                for (Map.Entry<String, Boolean> entry : pitIdToSucceededMap.entrySet()) {
                    deletePitResults.add(new DeletePitInfo(entry.getValue(), entry.getKey()));
                }
                DeletePitResponse deletePitResponse = new DeletePitResponse(deletePitResults);
                listener.onResponse(deletePitResponse);
            }

            @Override
            public void onFailure(final Exception e) {
                logger.error("Delete PITs failed", e);
                listener.onFailure(e);
            }
        }, size);
    }

    /**
     * This method returns indices associated for each pit
     */
    public Map<String, String[]> getIndicesForPits(List<String> pitIds) {
        Map<String, String[]> pitToIndicesMap = new HashMap<>();
        for (String pitId : pitIds) {
            pitToIndicesMap.put(pitId, SearchContextId.decode(nodeClient.getNamedWriteableRegistry(), pitId).getActualIndices());
        }
        return pitToIndicesMap;
    }

    /**
     * Get all active point in time contexts
     */
    public void getAllPits(ActionListener<GetAllPitNodesResponse> getAllPitsListener) {
        final List<DiscoveryNode> nodes = new ArrayList<>();
        for (final DiscoveryNode node : clusterService.state().nodes().getDataNodes().values()) {
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = nodes.toArray(new DiscoveryNode[0]);
        GetAllPitNodesRequest getAllPitNodesRequest = new GetAllPitNodesRequest(disNodesArr);
        transportService.sendRequest(
            transportService.getLocalNode(),
            GetAllPitsAction.NAME,
            getAllPitNodesRequest,
            new TransportResponseHandler<GetAllPitNodesResponse>() {

                @Override
                public void handleResponse(GetAllPitNodesResponse response) {
                    getAllPitsListener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    getAllPitsListener.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public GetAllPitNodesResponse read(StreamInput in) throws IOException {
                    return new GetAllPitNodesResponse(in);
                }
            }
        );
    }
}
