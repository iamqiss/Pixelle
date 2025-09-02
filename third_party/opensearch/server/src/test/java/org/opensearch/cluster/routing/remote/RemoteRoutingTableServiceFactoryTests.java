/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.cluster.routing.remote;

import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.repositories.RepositoriesService;
import org.density.repositories.fs.FsRepository;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.junit.After;

import java.util.function.Supplier;

import static org.density.gateway.remote.RemoteClusterStateService.REMOTE_PUBLICATION_SETTING_KEY;
import static org.density.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;

public class RemoteRoutingTableServiceFactoryTests extends DensityTestCase {

    Supplier<RepositoriesService> repositoriesService;
    private ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @After
    public void teardown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testGetServiceWhenRemoteRoutingDisabled() {
        Settings settings = Settings.builder().build();
        RemoteRoutingTableService service = RemoteRoutingTableServiceFactory.getService(
            repositoriesService,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            "test-cluster"
        );
        assertTrue(service instanceof NoopRemoteRoutingTableService);
    }

    public void testGetServiceWhenRemoteRoutingEnabled() {
        Settings settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .put(FsRepository.REPOSITORIES_COMPRESS_SETTING.getKey(), false)
            .put(REMOTE_PUBLICATION_SETTING_KEY, "true")
            .build();
        RemoteRoutingTableService service = RemoteRoutingTableServiceFactory.getService(
            repositoriesService,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            "test-cluster"
        );
        assertTrue(service instanceof InternalRemoteRoutingTableService);
    }

}
