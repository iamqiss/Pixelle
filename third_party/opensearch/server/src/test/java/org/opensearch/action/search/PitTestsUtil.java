/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.search;

import org.density.Version;
import org.density.action.admin.cluster.state.ClusterStateRequest;
import org.density.action.admin.cluster.state.ClusterStateResponse;
import org.density.action.admin.indices.segments.IndicesSegmentResponse;
import org.density.action.admin.indices.segments.PitSegmentsAction;
import org.density.action.admin.indices.segments.PitSegmentsRequest;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.action.ActionFuture;
import org.density.common.unit.TimeValue;
import org.density.common.util.concurrent.AtomicArray;
import org.density.core.index.shard.ShardId;
import org.density.index.query.IdsQueryBuilder;
import org.density.index.query.MatchAllQueryBuilder;
import org.density.index.query.QueryBuilder;
import org.density.index.query.TermQueryBuilder;
import org.density.search.SearchPhaseResult;
import org.density.search.SearchShardTarget;
import org.density.search.internal.AliasFilter;
import org.density.search.internal.ShardSearchContextId;
import org.density.transport.client.Client;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.density.test.DensityTestCase.between;
import static org.density.test.DensityTestCase.randomAlphaOfLength;
import static org.density.test.DensityTestCase.randomBoolean;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Helper class for common pit tests functions
 */
public class PitTestsUtil {
    private PitTestsUtil() {}

    public static QueryBuilder randomQueryBuilder() {
        if (randomBoolean()) {
            return new TermQueryBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
        } else if (randomBoolean()) {
            return new MatchAllQueryBuilder();
        } else {
            return new IdsQueryBuilder().addIds(randomAlphaOfLength(10));
        }
    }

    public static String getPitId() {
        AtomicArray<SearchPhaseResult> array = new AtomicArray<>(3);
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult1 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("a", 1),
            null
        );
        testSearchPhaseResult1.setSearchShardTarget(new SearchShardTarget("node_1", new ShardId("idx", "uuid1", 2), null, null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult2 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("b", 12),
            null
        );
        testSearchPhaseResult2.setSearchShardTarget(new SearchShardTarget("node_2", new ShardId("idy", "uuid2", 42), null, null));
        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult3 = new SearchAsyncActionTests.TestSearchPhaseResult(
            new ShardSearchContextId("c", 42),
            null
        );
        testSearchPhaseResult3.setSearchShardTarget(new SearchShardTarget("node_3", new ShardId("idy", "uuid2", 43), null, null));
        array.setOnce(0, testSearchPhaseResult1);
        array.setOnce(1, testSearchPhaseResult2);
        array.setOnce(2, testSearchPhaseResult3);

        final Version version = Version.CURRENT;
        final Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (SearchPhaseResult result : array.asList()) {
            final AliasFilter aliasFilter;
            if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder());
            } else if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder(), "alias-" + between(1, 10));
            } else {
                aliasFilter = AliasFilter.EMPTY;
            }
            if (randomBoolean()) {
                aliasFilters.put(result.getSearchShardTarget().getShardId().getIndex().getUUID(), aliasFilter);
            }
        }
        return SearchContextId.encode(array.asList(), aliasFilters, version);
    }

    public static void assertUsingGetAllPits(Client client, String id, long creationTime, TimeValue keepAlive) throws ExecutionException,
        InterruptedException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client.admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>();
        for (final DiscoveryNode node : clusterStateResponse.getState().nodes().getDataNodes().values()) {
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(disNodesArr);
        ActionFuture<GetAllPitNodesResponse> execute1 = client.execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest);
        GetAllPitNodesResponse getPitResponse = execute1.get();
        assertTrue(getPitResponse.getPitInfos().get(0).getPitId().contains(id));
        Assert.assertEquals(getPitResponse.getPitInfos().get(0).getCreationTime(), creationTime);
        Assert.assertEquals(getPitResponse.getPitInfos().get(0).getKeepAlive(), keepAlive.getMillis());
    }

    public static void assertGetAllPitsEmpty(Client client) throws ExecutionException, InterruptedException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client.admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>();
        for (final DiscoveryNode node : clusterStateResponse.getState().nodes().getDataNodes().values()) {
            nodes.add(node);
        }
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        GetAllPitNodesRequest getAllPITNodesRequest = new GetAllPitNodesRequest(disNodesArr);
        ActionFuture<GetAllPitNodesResponse> execute1 = client.execute(GetAllPitsAction.INSTANCE, getAllPITNodesRequest);
        GetAllPitNodesResponse getPitResponse = execute1.get();
        Assert.assertEquals(0, getPitResponse.getPitInfos().size());
    }

    public static void assertSegments(boolean isEmpty, String index, long expectedShardSize, Client client, String pitId) {
        PitSegmentsRequest pitSegmentsRequest;
        pitSegmentsRequest = new PitSegmentsRequest();
        List<String> pitIds = new ArrayList<>();
        pitIds.add(pitId);
        pitSegmentsRequest.clearAndSetPitIds(pitIds);
        IndicesSegmentResponse indicesSegmentResponse = client.execute(PitSegmentsAction.INSTANCE, pitSegmentsRequest).actionGet();
        assertTrue(indicesSegmentResponse.getShardFailures() == null || indicesSegmentResponse.getShardFailures().length == 0);
        assertEquals(indicesSegmentResponse.getIndices().isEmpty(), isEmpty);
        if (!isEmpty) {
            assertTrue(indicesSegmentResponse.getIndices().get(index) != null);
            assertTrue(indicesSegmentResponse.getIndices().get(index).getIndex().equalsIgnoreCase(index));
            assertEquals(expectedShardSize, indicesSegmentResponse.getIndices().get(index).getShards().size());
        }
    }

    public static void assertSegments(boolean isEmpty, String index, long expectedShardSize, Client client) {
        PitSegmentsRequest pitSegmentsRequest = new PitSegmentsRequest("_all");
        IndicesSegmentResponse indicesSegmentResponse = client.execute(PitSegmentsAction.INSTANCE, pitSegmentsRequest).actionGet();
        assertTrue(indicesSegmentResponse.getShardFailures() == null || indicesSegmentResponse.getShardFailures().length == 0);
        assertEquals(indicesSegmentResponse.getIndices().isEmpty(), isEmpty);
        if (!isEmpty) {
            assertTrue(indicesSegmentResponse.getIndices().get(index) != null);
            assertTrue(indicesSegmentResponse.getIndices().get(index).getIndex().equalsIgnoreCase(index));
            assertEquals(expectedShardSize, indicesSegmentResponse.getIndices().get(index).getShards().size());
        }
    }

    public static void assertSegments(boolean isEmpty, Client client) {
        assertSegments(isEmpty, "index", 2, client);
    }

    public static void assertSegments(boolean isEmpty, Client client, String pitId) {
        assertSegments(isEmpty, "index", 2, client, pitId);
    }
}
