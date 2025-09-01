/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.noop;

import org.density.common.settings.Settings;
import org.density.identity.IdentityService;
import org.density.identity.NamedPrincipal;
import org.density.identity.PluginSubject;
import org.density.plugins.IdentityAwarePlugin;
import org.density.plugins.Plugin;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class NoopPluginSubjectTests extends DensityTestCase {
    public static class TestPlugin extends Plugin implements IdentityAwarePlugin {
        private PluginSubject subject;

        @Override
        public void assignSubject(PluginSubject subject) {
            this.subject = subject;
        }

        public PluginSubject getSubject() {
            return subject;
        }
    }

    public void testInitializeIdentityAwarePlugin() throws Exception {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        IdentityService identityService = new IdentityService(Settings.EMPTY, threadPool, List.of());

        TestPlugin testPlugin = new TestPlugin();
        identityService.initializeIdentityAwarePlugins(List.of(testPlugin));

        PluginSubject testPluginSubject = new NoopPluginSubject(threadPool);
        assertThat(testPlugin.getSubject().getPrincipal().getName(), equalTo(NamedPrincipal.UNAUTHENTICATED.getName()));
        assertThat(testPluginSubject.getPrincipal().getName(), equalTo(NamedPrincipal.UNAUTHENTICATED.getName()));
        threadPool.getThreadContext().putHeader("test_header", "foo");
        assertThat(threadPool.getThreadContext().getHeader("test_header"), equalTo("foo"));
        testPluginSubject.runAs(() -> { assertNull(threadPool.getThreadContext().getHeader("test_header")); });
        assertThat(threadPool.getThreadContext().getHeader("test_header"), equalTo("foo"));
        terminate(threadPool);
    }
}
