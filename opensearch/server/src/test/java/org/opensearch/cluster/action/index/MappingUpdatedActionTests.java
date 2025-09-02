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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.cluster.action.index;

import org.density.Version;
import org.density.action.admin.indices.mapping.put.AutoPutMappingAction;
import org.density.action.support.PlainActionFuture;
import org.density.cluster.ClusterName;
import org.density.cluster.ClusterState;
import org.density.cluster.action.index.MappingUpdatedAction.AdjustableSemaphore;
import org.density.cluster.node.DiscoveryNode;
import org.density.cluster.node.DiscoveryNodes;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.core.action.ActionListener;
import org.density.core.index.Index;
import org.density.index.mapper.ContentPath;
import org.density.index.mapper.Mapper;
import org.density.index.mapper.Mapping;
import org.density.index.mapper.MetadataFieldMapper;
import org.density.index.mapper.RootObjectMapper;
import org.density.test.DensityTestCase;
import org.density.transport.client.AdminClient;
import org.density.transport.client.Client;
import org.density.transport.client.IndicesAdminClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.density.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MappingUpdatedActionTests extends DensityTestCase {

    public void testAdjustableSemaphore() {
        AdjustableSemaphore sem = new AdjustableSemaphore(1, randomBoolean());
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());

        // increase the number of max permits to 2
        sem.setMaxPermits(2);
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());

        // release all current permits
        sem.release();
        assertEquals(1, sem.availablePermits());
        sem.release();
        assertEquals(2, sem.availablePermits());

        // reduce number of max permits to 1
        sem.setMaxPermits(1);
        assertEquals(1, sem.availablePermits());
        // set back to 2
        sem.setMaxPermits(2);
        assertEquals(2, sem.availablePermits());

        // take both permits and reduce max permits
        assertTrue(sem.tryAcquire());
        assertTrue(sem.tryAcquire());
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());
        sem.setMaxPermits(1);
        assertEquals(-1, sem.availablePermits());
        assertFalse(sem.tryAcquire());

        // release one permit
        sem.release();
        assertEquals(0, sem.availablePermits());
        assertFalse(sem.tryAcquire());

        // release second permit
        sem.release();
        assertEquals(1, sem.availablePermits());
        assertTrue(sem.tryAcquire());
    }

    public void testMappingUpdatedActionBlocks() throws Exception {
        List<ActionListener<Void>> inFlightListeners = new CopyOnWriteArrayList<>();
        final MappingUpdatedAction mua = new MappingUpdatedAction(
            Settings.builder().put(MappingUpdatedAction.INDICES_MAX_IN_FLIGHT_UPDATES_SETTING.getKey(), 1).build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            null
        ) {

            @Override
            protected void sendUpdateMapping(Index index, Mapping mappingUpdate, ActionListener<Void> listener) {
                inFlightListeners.add(listener);
            }
        };

        PlainActionFuture<Void> fut1 = new PlainActionFuture<>();
        mua.updateMappingOnClusterManager(null, null, fut1);
        assertEquals(1, inFlightListeners.size());
        assertEquals(0, mua.blockedThreads());

        PlainActionFuture<Void> fut2 = new PlainActionFuture<>();
        Thread thread = new Thread(() -> {
            mua.updateMappingOnClusterManager(null, null, fut2); // blocked
        });
        thread.start();
        assertBusy(() -> assertEquals(1, mua.blockedThreads()));

        assertEquals(1, inFlightListeners.size());
        assertFalse(fut1.isDone());
        inFlightListeners.remove(0).onResponse(null);
        assertTrue(fut1.isDone());

        thread.join();
        assertEquals(0, mua.blockedThreads());
        assertEquals(1, inFlightListeners.size());
        assertFalse(fut2.isDone());
        inFlightListeners.remove(0).onResponse(null);
        assertTrue(fut2.isDone());
    }

    public void testSendUpdateMappingUsingAutoPutMappingAction() {
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("first", buildNewFakeTransportAddress(), Version.V_3_0_0))
            .build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).nodes(nodes).build();
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        Client client = mock(Client.class);
        when(client.admin()).thenReturn(adminClient);

        MappingUpdatedAction mua = new MappingUpdatedAction(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            clusterService
        );
        mua.setClient(client);

        Settings indexSettings = Settings.builder().put(SETTING_VERSION_CREATED, Version.CURRENT).build();
        final Mapper.BuilderContext context = new Mapper.BuilderContext(indexSettings, new ContentPath());
        RootObjectMapper rootObjectMapper = new RootObjectMapper.Builder("name").build(context);
        Mapping update = new Mapping(Version.V_3_0_0, rootObjectMapper, new MetadataFieldMapper[0], Map.of());

        mua.sendUpdateMapping(new Index("name", "uuid"), update, ActionListener.wrap(() -> {}));
        verify(indicesAdminClient).execute(eq(AutoPutMappingAction.INSTANCE), any(), any());
    }
}
