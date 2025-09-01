/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rest;

import org.density.plugin.wlm.WlmClusterSettingValuesProvider;
import org.density.plugin.wlm.WorkloadManagementTestUtils;
import org.density.rest.RestRequest;
import org.density.test.DensityTestCase;
import org.density.transport.client.node.NodeClient;

import static org.mockito.Mockito.mock;

public class RestUpdateWorkloadGroupActionTests extends DensityTestCase {

    public void testPrepareRequestThrowsWhenWlmModeDisabled() {
        try {
            WlmClusterSettingValuesProvider nonPluginSettingValuesProvider = WorkloadManagementTestUtils
                .setUpNonPluginSettingValuesProvider("disabled");
            RestUpdateWorkloadGroupAction restUpdateWorkloadGroupAction = new RestUpdateWorkloadGroupAction(nonPluginSettingValuesProvider);
            restUpdateWorkloadGroupAction.prepareRequest(mock(RestRequest.class), mock(NodeClient.class));
            fail("Expected exception when WLM mode is DISABLED");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update"));
        }
    }

    public void testPrepareRequestThrowsWhenWlmModeMonitorOnly() {
        try {
            WlmClusterSettingValuesProvider nonPluginSettingValuesProvider = WorkloadManagementTestUtils
                .setUpNonPluginSettingValuesProvider("monitor_only");
            RestUpdateWorkloadGroupAction restUpdateWorkloadGroupAction = new RestUpdateWorkloadGroupAction(nonPluginSettingValuesProvider);
            restUpdateWorkloadGroupAction.prepareRequest(mock(RestRequest.class), mock(NodeClient.class));
            fail("Expected exception when WLM mode is MONITOR_ONLY");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("update"));
        }
    }
}
