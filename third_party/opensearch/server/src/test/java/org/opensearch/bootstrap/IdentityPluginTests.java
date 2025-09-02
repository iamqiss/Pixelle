/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.bootstrap;

import org.density.DensityException;
import org.density.common.settings.Settings;
import org.density.identity.IdentityService;
import org.density.identity.noop.NoopIdentityPlugin;
import org.density.identity.noop.NoopTokenManager;
import org.density.plugins.IdentityPlugin;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class IdentityPluginTests extends DensityTestCase {

    public void testSingleIdentityPluginSucceeds() {
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        IdentityPlugin identityPlugin1 = new NoopIdentityPlugin(threadPool);
        List<IdentityPlugin> pluginList1 = List.of(identityPlugin1);
        IdentityService identityService1 = new IdentityService(Settings.EMPTY, threadPool, pluginList1);
        assertTrue(identityService1.getCurrentSubject().getPrincipal().getName().equalsIgnoreCase("Unauthenticated"));
        assertThat(identityService1.getTokenManager(), is(instanceOf(NoopTokenManager.class)));
        terminate(threadPool);
    }

    public void testMultipleIdentityPluginsFail() {
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        IdentityPlugin identityPlugin1 = new NoopIdentityPlugin(threadPool);
        IdentityPlugin identityPlugin2 = new NoopIdentityPlugin(threadPool);
        IdentityPlugin identityPlugin3 = new NoopIdentityPlugin(threadPool);
        List<IdentityPlugin> pluginList = List.of(identityPlugin1, identityPlugin2, identityPlugin3);
        Exception ex = assertThrows(DensityException.class, () -> new IdentityService(Settings.EMPTY, threadPool, pluginList));
        assert (ex.getMessage().contains("Multiple identity plugins are not supported,"));
        terminate(threadPool);
    }
}
