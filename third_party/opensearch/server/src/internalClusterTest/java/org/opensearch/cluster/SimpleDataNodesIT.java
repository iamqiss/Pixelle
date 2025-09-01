/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.cluster;

import org.density.action.UnavailableShardsException;
import org.density.action.admin.cluster.health.ClusterHealthResponse;
import org.density.action.index.IndexResponse;
import org.density.action.support.ActiveShardCount;
import org.density.cluster.health.ClusterHealthStatus;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.Priority;
import org.density.common.settings.Settings;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.test.DensityIntegTestCase;
import org.density.test.DensityIntegTestCase.ClusterScope;
import org.density.test.DensityIntegTestCase.Scope;
import org.density.transport.client.Requests;

import static org.density.common.unit.TimeValue.timeValueSeconds;
import static org.density.test.NodeRoles.dataNode;
import static org.density.test.NodeRoles.nonDataNode;
import static org.density.transport.client.Requests.createIndexRequest;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class SimpleDataNodesIT extends DensityIntegTestCase {

    private static final String SOURCE = "{\"type1\":{\"id\":\"1\",\"name\":\"test\"}}";

    public void testIndexingBeforeAndAfterDataNodesStart() {
        internalCluster().startNode(nonDataNode());
        client().admin().indices().create(createIndexRequest("test").waitForActiveShards(ActiveShardCount.NONE)).actionGet();
        try {
            client().index(Requests.indexRequest("test").id("1").source(SOURCE, MediaTypeRegistry.JSON).timeout(timeValueSeconds(1)))
                .actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        internalCluster().startNode(nonDataNode());
        assertThat(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("2")
                .setLocal(true)
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        // still no shard should be allocated
        try {
            client().index(Requests.indexRequest("test").id("1").source(SOURCE, MediaTypeRegistry.JSON).timeout(timeValueSeconds(1)))
                .actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        // now, start a node data, and see that it gets with shards
        internalCluster().startNode(dataNode());
        assertThat(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .setLocal(true)
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        IndexResponse indexResponse = client().index(Requests.indexRequest("test").id("1").source(SOURCE, MediaTypeRegistry.JSON))
            .actionGet();
        assertThat(indexResponse.getId(), equalTo("1"));
    }

    public void testShardsAllocatedAfterDataNodesStart() {
        internalCluster().startNode(nonDataNode());
        client().admin()
            .indices()
            .create(
                createIndexRequest("test").settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                    .waitForActiveShards(ActiveShardCount.NONE)
            )
            .actionGet();
        final ClusterHealthResponse healthResponse1 = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .execute()
            .actionGet();
        assertThat(healthResponse1.isTimedOut(), equalTo(false));
        assertThat(healthResponse1.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse1.getActiveShards(), equalTo(0));

        internalCluster().startNode(dataNode());

        assertThat(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("2")
                .setWaitForGreenStatus()
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );
    }

    public void testAutoExpandReplicasAdjustedWhenDataNodeJoins() {
        internalCluster().startNode(nonDataNode());
        client().admin()
            .indices()
            .create(
                createIndexRequest("test").settings(Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"))
                    .waitForActiveShards(ActiveShardCount.NONE)
            )
            .actionGet();
        final ClusterHealthResponse healthResponse1 = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .execute()
            .actionGet();
        assertThat(healthResponse1.isTimedOut(), equalTo(false));
        assertThat(healthResponse1.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse1.getActiveShards(), equalTo(0));

        internalCluster().startNode();
        internalCluster().startNode();
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();
    }

}
