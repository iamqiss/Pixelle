/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.support.nodes;

import org.density.action.admin.cluster.node.stats.NodesStatsRequest;
import org.density.action.admin.cluster.stats.ClusterStatsRequest;
import org.density.action.admin.cluster.stats.TransportClusterStatsAction;
import org.density.action.support.ActionFilters;
import org.density.action.support.PlainActionFuture;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.service.ClusterService;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.indices.IndicesService;
import org.density.node.NodeService;
import org.density.test.transport.CapturingTransport;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportClusterStatsActionTests extends TransportNodesActionTests {

    /**
     * In the optimized ClusterStats Request, we do not send the DiscoveryNodes List to each node. This behavior is
     * asserted in this test.
     */
    public void testClusterStatsActionWithoutRetentionOfDiscoveryNodesList() {
        ClusterStatsRequest request = new ClusterStatsRequest();
        Map<String, List<MockClusterStatsNodeRequest>> combinedSentRequest = performNodesInfoAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.getDiscoveryNodes()); });
        });
    }

    public void testClusterStatsActionWithPreFilledConcreteNodesAndWithoutRetentionOfDiscoveryNodesList() {
        ClusterStatsRequest request = new ClusterStatsRequest();
        Collection<DiscoveryNode> discoveryNodes = clusterService.state().getNodes().getNodes().values();
        request.setConcreteNodes(discoveryNodes.toArray(DiscoveryNode[]::new));
        Map<String, List<MockClusterStatsNodeRequest>> combinedSentRequest = performNodesInfoAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.getDiscoveryNodes()); });
        });
    }

    private Map<String, List<MockClusterStatsNodeRequest>> performNodesInfoAction(ClusterStatsRequest request) {
        TransportNodesAction action = getTestTransportClusterStatsAction();
        PlainActionFuture<NodesStatsRequest> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        Map<String, List<MockClusterStatsNodeRequest>> combinedSentRequest = new HashMap<>();

        capturedRequests.forEach((node, capturedRequestList) -> {
            List<MockClusterStatsNodeRequest> sentRequestList = new ArrayList<>();

            capturedRequestList.forEach(preSentRequest -> {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
                    TransportClusterStatsAction.ClusterStatsNodeRequest clusterStatsNodeRequestFromCoordinator =
                        (TransportClusterStatsAction.ClusterStatsNodeRequest) preSentRequest.request;
                    clusterStatsNodeRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
                    MockClusterStatsNodeRequest mockClusterStatsNodeRequest = new MockClusterStatsNodeRequest(in);
                    sentRequestList.add(mockClusterStatsNodeRequest);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            combinedSentRequest.put(node, sentRequestList);
        });

        return combinedSentRequest;
    }

    private TestTransportClusterStatsAction getTestTransportClusterStatsAction() {
        return new TestTransportClusterStatsAction(
            THREAD_POOL,
            clusterService,
            transportService,
            nodeService,
            indicesService,
            new ActionFilters(Collections.emptySet())
        );
    }

    private static class TestTransportClusterStatsAction extends TransportClusterStatsAction {
        public TestTransportClusterStatsAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            NodeService nodeService,
            IndicesService indicesService,
            ActionFilters actionFilters
        ) {
            super(threadPool, clusterService, transportService, nodeService, indicesService, actionFilters);
        }
    }

    private static class MockClusterStatsNodeRequest extends TransportClusterStatsAction.ClusterStatsNodeRequest {

        public MockClusterStatsNodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public DiscoveryNode[] getDiscoveryNodes() {
            return this.request.concreteNodes();
        }
    }
}
