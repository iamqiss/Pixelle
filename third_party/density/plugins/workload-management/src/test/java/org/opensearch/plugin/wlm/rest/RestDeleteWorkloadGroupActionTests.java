/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.rest;

import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.common.CheckedConsumer;
import org.density.common.unit.TimeValue;
import org.density.plugin.wlm.WlmClusterSettingValuesProvider;
import org.density.plugin.wlm.WorkloadManagementTestUtils;
import org.density.plugin.wlm.action.DeleteWorkloadGroupAction;
import org.density.plugin.wlm.action.DeleteWorkloadGroupRequest;
import org.density.rest.RestChannel;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.rest.action.RestToXContentListener;
import org.density.test.DensityTestCase;
import org.density.test.rest.FakeRestRequest;
import org.density.transport.client.node.NodeClient;

import java.util.List;

import org.mockito.ArgumentCaptor;

import static org.density.plugin.wlm.WorkloadManagementTestUtils.NAME_ONE;
import static org.density.rest.RestRequest.Method.DELETE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class RestDeleteWorkloadGroupActionTests extends DensityTestCase {
    /**
     * Test case to validate the construction for RestDeleteWorkloadGroupAction
     */
    public void testConstruction() {
        RestDeleteWorkloadGroupAction action = new RestDeleteWorkloadGroupAction(mock(WlmClusterSettingValuesProvider.class));
        assertNotNull(action);
        assertEquals("delete_workload_group", action.getName());
        List<RestHandler.Route> routes = action.routes();
        assertEquals(1, routes.size());
        RestHandler.Route route = routes.get(0);
        assertEquals(DELETE, route.getMethod());
        assertEquals("_wlm/workload_group/{name}", route.getPath());
    }

    /**
     * Test case to validate the prepareRequest logic for RestDeleteWorkloadGroupAction
     */
    @SuppressWarnings("unchecked")
    public void testPrepareRequest() throws Exception {
        RestDeleteWorkloadGroupAction restDeleteWorkloadGroupAction = new RestDeleteWorkloadGroupAction(
            mock(WlmClusterSettingValuesProvider.class)
        );
        NodeClient nodeClient = mock(NodeClient.class);
        RestRequest realRequest = new FakeRestRequest();
        realRequest.params().put("name", NAME_ONE);
        ;
        RestRequest spyRequest = spy(realRequest);

        doReturn(TimeValue.timeValueSeconds(30)).when(spyRequest).paramAsTime(eq("cluster_manager_timeout"), any(TimeValue.class));
        doReturn(TimeValue.timeValueSeconds(60)).when(spyRequest).paramAsTime(eq("timeout"), any(TimeValue.class));

        CheckedConsumer<RestChannel, Exception> consumer = restDeleteWorkloadGroupAction.prepareRequest(spyRequest, nodeClient);
        assertNotNull(consumer);
        ArgumentCaptor<DeleteWorkloadGroupRequest> requestCaptor = ArgumentCaptor.forClass(DeleteWorkloadGroupRequest.class);
        ArgumentCaptor<RestToXContentListener<AcknowledgedResponse>> listenerCaptor = ArgumentCaptor.forClass(RestToXContentListener.class);
        doNothing().when(nodeClient).execute(eq(DeleteWorkloadGroupAction.INSTANCE), requestCaptor.capture(), listenerCaptor.capture());

        consumer.accept(mock(RestChannel.class));
        DeleteWorkloadGroupRequest capturedRequest = requestCaptor.getValue();
        assertEquals(NAME_ONE, capturedRequest.getName());
        assertEquals(TimeValue.timeValueSeconds(30), capturedRequest.clusterManagerNodeTimeout());
        assertEquals(TimeValue.timeValueSeconds(60), capturedRequest.timeout());
        verify(nodeClient).execute(
            eq(DeleteWorkloadGroupAction.INSTANCE),
            any(DeleteWorkloadGroupRequest.class),
            any(RestToXContentListener.class)
        );
    }

    public void testPrepareRequestThrowsWhenWlmModeDisabled() throws Exception {
        try {
            WlmClusterSettingValuesProvider nonPluginSettingValuesProvider = WorkloadManagementTestUtils
                .setUpNonPluginSettingValuesProvider("disabled");
            RestDeleteWorkloadGroupAction restDeleteWorkloadGroupAction = new RestDeleteWorkloadGroupAction(nonPluginSettingValuesProvider);
            restDeleteWorkloadGroupAction.prepareRequest(mock(RestRequest.class), mock(NodeClient.class));
            fail("Expected exception when WLM mode is DISABLED");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("delete"));
        }
    }
}
