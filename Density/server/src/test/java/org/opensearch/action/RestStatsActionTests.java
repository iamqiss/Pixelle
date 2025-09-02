/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action;

import org.density.common.settings.Settings;
import org.density.common.settings.SettingsFilter;
import org.density.rest.action.admin.cluster.RestClusterStatsAction;
import org.density.rest.action.admin.cluster.RestNodesInfoAction;
import org.density.rest.action.admin.cluster.RestNodesStatsAction;
import org.density.test.DensityTestCase;
import org.density.test.rest.FakeRestRequest;
import org.density.threadpool.TestThreadPool;
import org.density.transport.client.node.NodeClient;
import org.junit.After;

import java.util.Collections;

public class RestStatsActionTests extends DensityTestCase {
    private final TestThreadPool threadPool = new TestThreadPool(RestStatsActionTests.class.getName());
    private final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testClusterStatsActionPrepareRequestNoError() {
        RestClusterStatsAction action = new RestClusterStatsAction();
        try {
            action.prepareRequest(new FakeRestRequest(), client);
        } catch (Throwable t) {
            fail(t.getMessage());
        }
    }

    public void testNodesStatsActionPrepareRequestNoError() {
        RestNodesStatsAction action = new RestNodesStatsAction();
        try {
            action.prepareRequest(new FakeRestRequest(), client);
        } catch (Throwable t) {
            fail(t.getMessage());
        }
    }

    public void testNodesInfoActionPrepareRequestNoError() {
        RestNodesInfoAction action = new RestNodesInfoAction(new SettingsFilter(Collections.singleton("foo.filtered")));
        try {
            action.prepareRequest(new FakeRestRequest(), client);
        } catch (Throwable t) {
            fail(t.getMessage());
        }
    }
}
