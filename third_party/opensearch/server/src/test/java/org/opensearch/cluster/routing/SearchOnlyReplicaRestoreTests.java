/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.routing;

import org.density.Version;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.metadata.Metadata;
import org.density.common.UUIDs;
import org.density.common.settings.Settings;
import org.density.repositories.IndexId;
import org.density.snapshots.Snapshot;
import org.density.snapshots.SnapshotId;
import org.density.test.DensityTestCase;

import java.util.HashSet;

public class SearchOnlyReplicaRestoreTests extends DensityTestCase {

    public void testSearchOnlyReplicasRestored() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .numberOfSearchReplicas(1)
            )
            .build();

        IndexMetadata indexMetadata = metadata.index("test");
        RecoverySource.SnapshotRecoverySource snapshotRecoverySource = new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())),
            Version.CURRENT,
            new IndexId("test", UUIDs.randomBase64UUID(random()))
        );

        RoutingTable routingTable = RoutingTable.builder().addAsNewRestore(indexMetadata, snapshotRecoverySource, new HashSet<>()).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        IndexShardRoutingTable indexShardRoutingTable = clusterState.routingTable().index("test").shard(0);

        assertEquals(1, clusterState.routingTable().index("test").shards().size());
        assertEquals(3, indexShardRoutingTable.getShards().size());
        assertEquals(1, indexShardRoutingTable.searchOnlyReplicas().size());
    }
}
