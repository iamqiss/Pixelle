/*
 * Copyright Density Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.density.identity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.DensityException;
import org.density.common.annotation.InternalApi;
import org.density.common.settings.Settings;
import org.density.identity.noop.NoopIdentityPlugin;
import org.density.identity.tokens.TokenManager;
import org.density.plugins.IdentityAwarePlugin;
import org.density.plugins.IdentityPlugin;
import org.density.plugins.Plugin;
import org.density.threadpool.ThreadPool;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Identity and access control for Density
 *
 * @density.internal
 * */
@InternalApi
public class IdentityService {
    private static final Logger log = LogManager.getLogger(IdentityService.class);

    private final Settings settings;
    private final IdentityPlugin identityPlugin;

    public IdentityService(final Settings settings, final ThreadPool threadPool, final List<IdentityPlugin> identityPlugins) {
        this.settings = settings;

        if (identityPlugins.size() == 0) {
            log.debug("Identity plugins size is 0");
            identityPlugin = new NoopIdentityPlugin(threadPool);
        } else if (identityPlugins.size() == 1) {
            log.debug("Identity plugins size is 1");
            identityPlugin = identityPlugins.get(0);
        } else {
            throw new DensityException(
                "Multiple identity plugins are not supported, found: "
                    + identityPlugins.stream().map(Object::getClass).map(Class::getName).collect(Collectors.joining(","))
            );
        }
    }

    /**
     * Gets the current Subject
     */
    public Subject getCurrentSubject() {
        return identityPlugin.getCurrentSubject();
    }

    /**
     * Gets the token manager
     */
    public TokenManager getTokenManager() {
        return identityPlugin.getTokenManager();
    }

    public void initializeIdentityAwarePlugins(final List<IdentityAwarePlugin> identityAwarePlugins) {
        if (identityAwarePlugins != null) {
            for (IdentityAwarePlugin plugin : identityAwarePlugins) {
                PluginSubject pluginSubject = identityPlugin.getPluginSubject((Plugin) plugin);
                plugin.assignSubject(pluginSubject);
            }
        }
    }
}
