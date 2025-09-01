/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.shard;

import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodeRole;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.routing.ShardRouting;
import org.density.node.remotestore.RemoteStoreNodeAttribute;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexShardTestUtils {
    public static final String MOCK_STATE_REPO_NAME = "state-test-repo";
    public static final String MOCK_SEGMENT_REPO_NAME = "segment-test-repo";
    public static final String MOCK_TLOG_REPO_NAME = "tlog-test-repo";

    public static DiscoveryNode getFakeDiscoNode(String id) {
        return new DiscoveryNode(
            id,
            id,
            IndexShardTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }

    public static DiscoveryNode getFakeRemoteEnabledNode(String id) {
        Map<String, String> remoteNodeAttributes = new HashMap<String, String>();
        remoteNodeAttributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, MOCK_STATE_REPO_NAME);
        remoteNodeAttributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, MOCK_SEGMENT_REPO_NAME);
        remoteNodeAttributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, MOCK_TLOG_REPO_NAME);
        return new DiscoveryNode(
            id,
            id,
            IndexShardTestCase.buildNewFakeTransportAddress(),
            remoteNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
    }

    public static DiscoveryNodes getFakeDiscoveryNodes(List<ShardRouting> shardRoutings) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (ShardRouting routing : shardRoutings) {
            builder.add(getFakeDiscoNode(routing.currentNodeId()));
        }
        return builder.build();
    }

    public static DiscoveryNodes getFakeRemoteEnabledDiscoveryNodes(List<ShardRouting> shardRoutings) {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (ShardRouting routing : shardRoutings) {
            builder.add(getFakeRemoteEnabledNode(routing.currentNodeId()));
        }
        return builder.build();
    }

    public static DiscoveryNodes getFakeDiscoveryNodes(ShardRouting shardRouting) {
        return DiscoveryNodes.builder().add(getFakeDiscoNode(shardRouting.currentNodeId())).build();
    }
}
