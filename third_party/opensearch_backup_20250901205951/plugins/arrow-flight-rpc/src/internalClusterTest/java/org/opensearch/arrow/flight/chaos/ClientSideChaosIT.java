/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.arrow.flight.chaos;

import org.density.action.admin.indices.create.CreateIndexRequest;
import org.density.action.bulk.BulkRequest;
import org.density.action.index.IndexRequest;
import org.density.action.search.SearchResponse;
import org.density.arrow.flight.transport.FlightStreamPlugin;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.common.xcontent.XContentType;
import org.density.core.action.ActionListener;
import org.density.plugins.Plugin;
import org.density.test.DensityIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.density.common.util.FeatureFlags.STREAM_TRANSPORT;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.SUITE, minNumDataNodes = 3, maxNumDataNodes = 3)
public class ClientSideChaosIT extends DensityIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(FlightStreamPlugin.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        internalCluster().ensureAtLeastNumDataNodes(3);

        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 3)
            .put("index.number_of_replicas", 0) // Add replicas for failover testing
            .build();

        CreateIndexRequest createIndexRequest = new CreateIndexRequest("client-chaos-index").settings(indexSettings);
        client().admin().indices().create(createIndexRequest).actionGet();
        client().admin()
            .cluster()
            .prepareHealth("client-chaos-index")
            .setWaitForYellowStatus()
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        BulkRequest bulkRequest = new BulkRequest();
        for (int i = 0; i < 100; i++) {
            bulkRequest.add(new IndexRequest("client-chaos-index").source(XContentType.JSON, "field1", "value" + i, "field2", i));
        }
        client().bulk(bulkRequest).actionGet();
        client().admin().indices().prepareRefresh("client-chaos-index").get();
        ensureSearchable("client-chaos-index");
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    @AwaitsFix(bugUrl = "")
    public void testResponseTimeoutScenario() throws Exception {
        ChaosScenario.setTimeoutDelay(5000); // 5 second delay
        ChaosScenario.enableScenario(ChaosScenario.ClientFailureScenario.RESPONSE_TIMEOUT);

        try {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean timeout = new AtomicBoolean(false);
            client().prepareStreamSearch("client-chaos-index")
                .setTimeout(TimeValue.timeValueNanos(100))
                .execute(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        timeout.set(searchResponse.isTimedOut());
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {}
                });
            assertTrue(latch.await(15, TimeUnit.SECONDS));
            assertTrue("Should have response timeout", timeout.get());
        } finally {
            ChaosScenario.disable();
        }
    }
}
