/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.support.clustermanager.term;

import org.density.cluster.service.ClusterService;
import org.density.test.DensitySingleNodeTestCase;

import java.util.concurrent.ExecutionException;

public class ClusterTermVersionTests extends DensitySingleNodeTestCase {

    public void testTransportTermResponse() throws ExecutionException, InterruptedException {
        GetTermVersionRequest request = new GetTermVersionRequest();
        GetTermVersionResponse resp = client().execute(GetTermVersionAction.INSTANCE, request).get();

        final ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        assertTrue(resp.matches(clusterService.state()));
    }
}
