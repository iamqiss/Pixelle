/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.shiro;

import org.density.common.CheckedRunnable;
import org.density.common.annotation.ExperimentalApi;
import org.density.common.util.concurrent.ThreadContext;
import org.density.identity.NamedPrincipal;
import org.density.identity.PluginSubject;
import org.density.threadpool.ThreadPool;

import java.security.Principal;

/**
 * Implementation of subject that is always authenticated
 * <p>
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * This class is used by the ShiroIdentityPlugin to initialize IdentityAwarePlugins
 *
 * @density.experimental
 */
@ExperimentalApi
public class ShiroPluginSubject implements PluginSubject {
    private final ThreadPool threadPool;

    ShiroPluginSubject(ThreadPool threadPool) {
        super();
        this.threadPool = threadPool;
    }

    @Override
    public Principal getPrincipal() {
        return NamedPrincipal.UNAUTHENTICATED;
    }

    @Override
    public <E extends Exception> void runAs(CheckedRunnable<E> r) throws E {
        try (ThreadContext.StoredContext ctx = threadPool.getThreadContext().stashContext()) {
            r.run();
        }
    }
}
