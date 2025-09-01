/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.support.nodes;

import org.density.action.admin.cluster.node.stats.NodesStatsRequest;
import org.density.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.density.action.support.ActionFilters;
import org.density.action.support.PlainActionFuture;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.service.ClusterService;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.node.NodeService;
import org.density.test.transport.CapturingTransport;
import org.density.threadpool.ThreadPool;
import org.density.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportNodesStatsActionTests extends TransportNodesActionTests {

    /**
     * We don't want to send discovery nodes list to each request that is sent across from the coordinator node.
     * This behavior is asserted in this test.
     */
    public void testNodesStatsActionWithoutRetentionOfDiscoveryNodesList() {
        NodesStatsRequest request = new NodesStatsRequest();
        Map<String, List<MockNodeStatsRequest>> combinedSentRequest = performNodesStatsAction(request);

        assertNotNull(combinedSentRequest);
        combinedSentRequest.forEach((node, capturedRequestList) -> {
            assertNotNull(capturedRequestList);
            capturedRequestList.forEach(sentRequest -> { assertNull(sentRequest.getDiscoveryNodes()); });
        });
    }

    private Map<String, List<MockNodeStatsRequest>> performNodesStatsAction(NodesStatsRequest request) {
        TransportNodesAction action = getTestTransportNodesStatsAction();
        PlainActionFuture<NodesStatsRequest> listener = new PlainActionFuture<>();
        action.new AsyncAction(null, request, listener).start();
        Map<String, List<CapturingTransport.CapturedRequest>> capturedRequests = transport.getCapturedRequestsByTargetNodeAndClear();
        Map<String, List<MockNodeStatsRequest>> combinedSentRequest = new HashMap<>();

        capturedRequests.forEach((node, capturedRequestList) -> {
            List<MockNodeStatsRequest> sentRequestList = new ArrayList<>();

            capturedRequestList.forEach(preSentRequest -> {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
                    TransportNodesStatsAction.NodeStatsRequest nodesStatsRequestFromCoordinator =
                        (TransportNodesStatsAction.NodeStatsRequest) preSentRequest.request;
                    nodesStatsRequestFromCoordinator.writeTo(out);
                    StreamInput in = out.bytes().streamInput();
                    MockNodeStatsRequest nodesStatsRequest = new MockNodeStatsRequest(in);
                    sentRequestList.add(nodesStatsRequest);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            combinedSentRequest.put(node, sentRequestList);
        });

        return combinedSentRequest;
    }

    private TestTransportNodesStatsAction getTestTransportNodesStatsAction() {
        return new TestTransportNodesStatsAction(
            THREAD_POOL,
            clusterService,
            transportService,
            nodeService,
            new ActionFilters(Collections.emptySet())
        );
    }

    private static class TestTransportNodesStatsAction extends TransportNodesStatsAction {
        public TestTransportNodesStatsAction(
            ThreadPool threadPool,
            ClusterService clusterService,
            TransportService transportService,
            NodeService nodeService,
            ActionFilters actionFilters
        ) {
            super(threadPool, clusterService, transportService, nodeService, actionFilters);
        }
    }

    private static class MockNodeStatsRequest extends TransportNodesStatsAction.NodeStatsRequest {

        public MockNodeStatsRequest(StreamInput in) throws IOException {
            super(in);
        }

        public DiscoveryNode[] getDiscoveryNodes() {
            return this.request.concreteNodes();
        }
    }
}
