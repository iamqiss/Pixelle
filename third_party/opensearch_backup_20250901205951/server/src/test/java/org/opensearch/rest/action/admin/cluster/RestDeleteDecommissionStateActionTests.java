/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rest.action.admin.cluster;

import org.density.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.List;

public class RestDeleteDecommissionStateActionTests extends RestActionTestCase {

    private RestDeleteDecommissionStateAction action;

    @Before
    public void setupAction() {
        action = new RestDeleteDecommissionStateAction();
        controller().registerHandler(action);
    }

    public void testRoutes() {
        List<RestHandler.Route> routes = action.routes();
        RestHandler.Route route = routes.get(0);
        assertEquals(route.getMethod(), RestRequest.Method.DELETE);
        assertEquals("/_cluster/decommission/awareness", route.getPath());
    }

    public void testCreateRequest() {
        DeleteDecommissionStateRequest request = action.createRequest();
        assertNotNull(request);
    }
}
