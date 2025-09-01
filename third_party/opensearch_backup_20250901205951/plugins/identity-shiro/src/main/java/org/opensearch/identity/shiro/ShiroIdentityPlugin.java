/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.identity.shiro;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.SecurityManager;
import org.density.cluster.metadata.IndexNameExpressionResolver;
import org.density.cluster.service.ClusterService;
import org.density.common.settings.Settings;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.env.Environment;
import org.density.env.NodeEnvironment;
import org.density.identity.PluginSubject;
import org.density.identity.Subject;
import org.density.identity.tokens.AuthToken;
import org.density.identity.tokens.TokenManager;
import org.density.plugins.ActionPlugin;
import org.density.plugins.IdentityPlugin;
import org.density.plugins.Plugin;
import org.density.repositories.RepositoriesService;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.script.ScriptService;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.Client;
import org.density.transport.client.node.NodeClient;
import org.density.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * Identity implementation with Shiro
 */
public final class ShiroIdentityPlugin extends Plugin implements IdentityPlugin, ActionPlugin {
    private Logger log = LogManager.getLogger(this.getClass());

    private final Settings settings;
    private final ShiroTokenManager authTokenHandler;

    private ThreadPool threadPool;

    /**
     * Create a new instance of the Shiro Identity Plugin
     *
     * @param settings settings being used in the configuration
     */
    public ShiroIdentityPlugin(final Settings settings) {
        this.settings = settings;
        authTokenHandler = new ShiroTokenManager();

        SecurityManager securityManager = new ShiroSecurityManager();
        SecurityUtils.setSecurityManager(securityManager);
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver expressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.threadPool = threadPool;
        return Collections.emptyList();
    }

    /**
     * Return a Shiro Subject based on the provided authTokenHandler and current subject
     *
     * @return The current subject
     */
    @Override
    public Subject getCurrentSubject() {
        return new ShiroSubject(authTokenHandler, SecurityUtils.getSubject());
    }

    /**
     * Return the Shiro Token Handler
     *
     * @return the Shiro Token Handler
     */
    @Override
    public TokenManager getTokenManager() {
        return this.authTokenHandler;
    }

    @Override
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext) {
        return AuthcRestHandler::new;
    }

    class AuthcRestHandler extends RestHandler.Wrapper {

        public AuthcRestHandler(RestHandler original) {
            super(original);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
            try {
                final AuthToken token = ShiroTokenExtractor.extractToken(request);
                // If no token was found, continue executing the request
                if (token == null) {
                    // Authentication did not fail so return true. Authorization is handled at the action level.
                    super.handleRequest(request, channel, client);
                    return;
                }
                ShiroSubject shiroSubject = (ShiroSubject) getCurrentSubject();
                shiroSubject.authenticate(token);
                // Caller was authorized, forward the request to the handler
                super.handleRequest(request, channel, client);
            } catch (final Exception e) {
                final BytesRestResponse bytesRestResponse = new BytesRestResponse(RestStatus.UNAUTHORIZED, e.getMessage());
                channel.sendResponse(bytesRestResponse);
            }
        }
    }

    public PluginSubject getPluginSubject(Plugin plugin) {
        return new ShiroPluginSubject(threadPool);
    }
}
