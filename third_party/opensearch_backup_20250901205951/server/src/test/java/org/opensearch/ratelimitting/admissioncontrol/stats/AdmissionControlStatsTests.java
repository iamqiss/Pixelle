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
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

public class AdmissionControlStatsTests extends DensityTestCase {
    AdmissionController admissionController;
    AdmissionControllerStats admissionControllerStats;
    AdmissionControlStats admissionControlStats;
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
        admissionController = new CpuBasedAdmissionController(
            CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
            mock(ResourceUsageCollectorService.class),
            clusterService,
            settings
        );
        admissionControllerStats = new AdmissionControllerStats(admissionController);
        List<AdmissionControllerStats> admissionControllerStats = new ArrayList<>();
        admissionControlStats = new AdmissionControlStats(admissionControllerStats);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testDefaults() throws IOException {
        assertEquals(admissionControlStats.getAdmissionControllerStatsList().size(), 0);
    }

    public void testRejectionCount() throws IOException {
        admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 11);
        admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 1);
        admissionControllerStats = new AdmissionControllerStats(admissionController);
        admissionControlStats = new AdmissionControlStats(List.of(admissionControllerStats));
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        builder = admissionControlStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String response = builder.toString();
        assertEquals(
            response,
            "{\"admission_control\":{\"global_cpu_usage\":{\"transport\":{\"rejection_count\":{\"search\":11,\"indexing\":1}}}}}"
        );
        AdmissionControlStats admissionControlStats1 = admissionControlStats;
        assertEquals(admissionControlStats.hashCode(), admissionControlStats1.hashCode());
    }
}
