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

package org.density.indices.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.action.admin.cluster.health.ClusterHealthResponse;
import org.density.action.admin.cluster.state.ClusterStateResponse;
import org.density.action.admin.indices.create.CreateIndexResponse;
import org.density.action.admin.indices.open.OpenIndexResponse;
import org.density.action.support.ActiveShardCount;
import org.density.cluster.health.ClusterHealthStatus;
import org.density.cluster.metadata.IndexMetadata;
import org.density.cluster.routing.ShardRoutingState;
import org.density.common.settings.Settings;
import org.density.index.IndexNotFoundException;
import org.density.indices.IndexClosedException;
import org.density.test.DensityIntegTestCase;

import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@DensityIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class SimpleIndexStateIT extends DensityIntegTestCase {
    private final Logger logger = LogManager.getLogger(SimpleIndexStateIT.class);

    public void testSimpleOpenClose() {
        logger.info("--> creating test index");
        createIndex("test");

        logger.info("--> waiting for green status");
        ensureGreen();

        NumShards numShards = getNumShards("test");

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertEquals(
            stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            numShards.totalNumShards
        );

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();

        logger.info("--> closing test index...");
        assertAcked(client().admin().indices().prepareClose("test"));

        stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.CLOSE));
        assertThat(stateResponse.getState().routingTable().index("test"), notNullValue());

        logger.info("--> trying to index into a closed index ...");
        try {
            client().prepareIndex("test").setId("1").setSource("field1", "value1").get();
            fail();
        } catch (IndexClosedException e) {
            // all is well
        }

        logger.info("--> opening index...");
        OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen("test").get();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));

        logger.info("--> waiting for green status");
        ensureGreen();

        stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));

        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertEquals(
            stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            numShards.totalNumShards
        );

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();
    }

    public void testFastCloseAfterCreateContinuesCreateAfterOpen() {
        logger.info("--> creating test index that cannot be allocated");
        client().admin()
            .indices()
            .prepareCreate("test")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(Settings.builder().put("index.routing.allocation.include.tag", "no_such_node").build())
            .get();

        ClusterHealthResponse health = client().admin().cluster().prepareHealth("test").setWaitForNodes(">=2").get();
        assertThat(health.isTimedOut(), equalTo(false));
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.RED));

        assertAcked(client().admin().indices().prepareClose("test").setWaitForActiveShards(ActiveShardCount.NONE));

        logger.info("--> updating test index settings to allow allocation");
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(Settings.builder().put("index.routing.allocation.include.tag", "").build())
            .get();

        client().admin().indices().prepareOpen("test").get();

        logger.info("--> waiting for green status");
        ensureGreen();

        NumShards numShards = getNumShards("test");

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().get();
        assertThat(stateResponse.getState().metadata().index("test").getState(), equalTo(IndexMetadata.State.OPEN));
        assertThat(stateResponse.getState().routingTable().index("test").shards().size(), equalTo(numShards.numPrimaries));
        assertEquals(
            stateResponse.getState().routingTable().index("test").shardsWithState(ShardRoutingState.STARTED).size(),
            numShards.totalNumShards
        );

        logger.info("--> indexing a simple document");
        client().prepareIndex("test").setId("1").setSource("field1", "value1").get();
    }

    public void testConsistencyAfterIndexCreationFailure() {
        logger.info("--> deleting test index....");
        try {
            client().admin().indices().prepareDelete("test").get();
        } catch (IndexNotFoundException ex) {
            // Ignore
        }

        logger.info("--> creating test index with invalid settings ");
        try {
            client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", "bad")).get();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [bad] for setting [index.number_of_shards]", ex.getMessage());
            // Expected
        }

        logger.info("--> creating test index with valid settings ");
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test")
            .setSettings(Settings.builder().put("number_of_shards", 1))
            .get();
        assertThat(response.isAcknowledged(), equalTo(true));
    }
}
