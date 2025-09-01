/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.ratelimitting.admissioncontrol.stats;

import org.density.cluster.service.ClusterService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.xcontent.json.JsonXContent;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.node.ResourceUsageCollectorService;
import org.density.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.density.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.density.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.density.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.density.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings;
import org.density.test.ClusterServiceUtils;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;

import java.io.IOException;

import static org.mockito.Mockito.mock;

public class AdmissionControllerStatsTests extends DensityTestCase {
    AdmissionController admissionController;
    AdmissionControllerStats admissionControllerStats;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder()
            .put(
                CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(),
                AdmissionControlMode.ENFORCED.getMode()
            )
            .build();
        threadPool = new TestThreadPool("admission_controller_settings_test");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        admissionController = new CpuBasedAdmissionController("TEST", mock(ResourceUsageCollectorService.class), clusterService, settings);
        admissionControllerStats = new AdmissionControllerStats(admissionController);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testDefaults() throws IOException {
        assertEquals(admissionControllerStats.getRejectionCount().size(), 0);
        assertEquals(admissionControllerStats.getAdmissionControllerName(), "TEST");
    }

    public void testRejectionCount() throws IOException {
        admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 11);
        admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 1);
        admissionControllerStats = new AdmissionControllerStats(admissionController);
        long searchRejection = admissionControllerStats.getRejectionCount().getOrDefault(AdmissionControlActionType.SEARCH.getType(), 0L);
        long indexingRejection = admissionControllerStats.getRejectionCount()
            .getOrDefault(AdmissionControlActionType.INDEXING.getType(), 0L);
        assertEquals(searchRejection, 11);
        assertEquals(indexingRejection, 1);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder = admissionControllerStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String response = builder.toString();
        assertEquals(response, "{\"transport\":{\"rejection_count\":{\"search\":11,\"indexing\":1}}}");
        AdmissionControllerStats admissionControllerStats1 = admissionControllerStats;
        assertEquals(admissionControllerStats.hashCode(), admissionControllerStats1.hashCode());
    }
}
