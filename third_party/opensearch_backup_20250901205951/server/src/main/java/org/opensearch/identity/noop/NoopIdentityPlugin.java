/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.noop;

import org.density.identity.PluginSubject;
import org.density.identity.Subject;
import org.density.identity.tokens.TokenManager;
import org.density.plugins.IdentityPlugin;
import org.density.plugins.Plugin;
import org.density.threadpool.ThreadPool;

/**
 * Implementation of identity plugin that does not enforce authentication or authorization
 * <p>
 * This class and related classes in this package will not return nulls or fail access checks
 *
 * @density.internal
 */
public class NoopIdentityPlugin implements IdentityPlugin {

    private final ThreadPool threadPool;

    public NoopIdentityPlugin(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * Get the current subject
     * @return Must never return null
     */
    @Override
    public Subject getCurrentSubject() {
        return new NoopSubject();
    }

    /**
     * Get a new NoopTokenManager
     * @return Must never return null
     */
    @Override
    public TokenManager getTokenManager() {
        return new NoopTokenManager();
    }

    @Override
    public PluginSubject getPluginSubject(Plugin plugin) {
        return new NoopPluginSubject(threadPool);
    }
}
