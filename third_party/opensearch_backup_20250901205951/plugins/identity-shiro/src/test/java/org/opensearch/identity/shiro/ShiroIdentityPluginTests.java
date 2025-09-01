/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.shiro;

import org.density.DensityException;
import org.density.common.settings.Settings;
import org.density.identity.IdentityService;
import org.density.plugins.IdentityPlugin;
import org.density.test.DensityTestCase;
import org.density.threadpool.ThreadPool;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

public class ShiroIdentityPluginTests extends DensityTestCase {

    public void testSingleIdentityPluginSucceeds() {
        IdentityPlugin identityPlugin1 = new ShiroIdentityPlugin(Settings.EMPTY);
        List<IdentityPlugin> pluginList1 = List.of(identityPlugin1);
        IdentityService identityService1 = new IdentityService(Settings.EMPTY, mock(ThreadPool.class), pluginList1);
        assertThat(identityService1.getTokenManager(), is(instanceOf(ShiroTokenManager.class)));
    }

    public void testMultipleIdentityPluginsFail() {
        IdentityPlugin identityPlugin1 = new ShiroIdentityPlugin(Settings.EMPTY);
        IdentityPlugin identityPlugin2 = new ShiroIdentityPlugin(Settings.EMPTY);
        IdentityPlugin identityPlugin3 = new ShiroIdentityPlugin(Settings.EMPTY);
        List<IdentityPlugin> pluginList = List.of(identityPlugin1, identityPlugin2, identityPlugin3);
        Exception ex = assertThrows(
            DensityException.class,
            () -> new IdentityService(Settings.EMPTY, mock(ThreadPool.class), pluginList)
        );
        assert (ex.getMessage().contains("Multiple identity plugins are not supported,"));
    }

}
