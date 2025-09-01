/*
 * Copyright 2015-2017 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.transport.netty4.ssl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.density.ExceptionsHelper;
import org.density.DensitySecurityException;
import org.density.Version;
import org.density.cluster.node.DiscoveryNode;
import org.density.common.SuppressForbidden;
import org.density.common.network.NetworkModule;
import org.density.common.network.NetworkService;
import org.density.common.settings.Settings;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.plugins.SecureTransportSettingsProvider;
import org.density.plugins.TransportExceptionHandler;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;
import org.density.transport.SharedGroupFactory;
import org.density.transport.TcpChannel;
import org.density.transport.netty4.Netty4Transport;
import org.density.transport.netty4.ssl.SecureConnectionTestUtil.SSLConnectionTestResult;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.SslHandler;

/**
 * @see <a href="https://github.com/density-project/security/blob/d526c9f6c2a438c14db8b413148204510b9fe2e2/src/main/java/org/density/security/ssl/transport/SecuritySSLNettyTransport.java">SecuritySSLNettyTransport</a>
 */
public class SecureNetty4Transport extends Netty4Transport {

    private static final Logger logger = LogManager.getLogger(SecureNetty4Transport.class);
    private final SecureTransportSettingsProvider secureTransportSettingsProvider;
    private final TransportExceptionHandler exceptionHandler;

    public SecureNetty4Transport(
        final Settings settings,
        final Version version,
        final ThreadPool threadPool,
        final NetworkService networkService,
        final PageCacheRecycler pageCacheRecycler,
        final NamedWriteableRegistry namedWriteableRegistry,
        final CircuitBreakerService circuitBreakerService,
        final SharedGroupFactory sharedGroupFactory,
        final SecureTransportSettingsProvider secureTransportSettingsProvider,
        final Tracer tracer
    ) {
        super(
            settings,
            version,
            threadPool,
            networkService,
            pageCacheRecycler,
            namedWriteableRegistry,
            circuitBreakerService,
            sharedGroupFactory,
            tracer
        );

        this.secureTransportSettingsProvider = secureTransportSettingsProvider;
        this.exceptionHandler = secureTransportSettingsProvider.buildServerTransportExceptionHandler(settings, this)
            .orElse(TransportExceptionHandler.NOOP);
    }

    @Override
    public void onException(TcpChannel channel, Exception e) {

        Throwable cause = e;

        if (e instanceof DecoderException && e != null) {
            cause = e.getCause();
        }

        exceptionHandler.onError(cause);
        logger.error("Exception during establishing a SSL connection: " + cause, cause);

        if (channel == null || !channel.isOpen()) {
            throw new DensitySecurityException("The provided TCP channel is invalid.", e);
        }
        super.onException(channel, e);
    }

    @Override
    protected ChannelHandler getServerChannelInitializer(String name) {
        return new SSLServerChannelInitializer(name);
    }

    @Override
    protected ChannelHandler getClientChannelInitializer(DiscoveryNode node) {
        return new SSLClientChannelInitializer(node);
    }

    protected class SSLServerChannelInitializer extends Netty4Transport.ServerChannelInitializer {

        public SSLServerChannelInitializer(String name) {
            super(name);
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);

            final boolean dualModeEnabled = secureTransportSettingsProvider.parameters(settings)
                .map(SecureTransportSettingsProvider.SecureTransportParameters::dualModeEnabled)
                .orElse(false);
            if (dualModeEnabled) {
                logger.info("SSL Dual mode enabled, using port unification handler");
                final ChannelHandler portUnificationHandler = new DualModeSslHandler(
                    settings,
                    secureTransportSettingsProvider,
                    SecureNetty4Transport.this
                );
                ch.pipeline().addFirst("port_unification_handler", portUnificationHandler);
            } else {
                final SSLEngine sslEngine = secureTransportSettingsProvider.buildSecureServerTransportEngine(
                    settings,
                    SecureNetty4Transport.this
                ).orElseGet(SslUtils::createDefaultServerSSLEngine);
                final SslHandler sslHandler = new SslHandler(sslEngine);
                ch.pipeline().addFirst("ssl_server", sslHandler);
            }
        }

        @Override
        public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof DecoderException && cause != null) {
                cause = cause.getCause();
            }

            logger.error("Exception during establishing a SSL connection: " + cause, cause);

            super.exceptionCaught(ctx, cause);
        }
    }

    protected static class ClientSSLHandler extends ChannelOutboundHandlerAdapter {
        private final Logger log = LogManager.getLogger(this.getClass());
        private final Settings settings;
        private final SecureTransportSettingsProvider secureTransportSettingsProvider;
        private final boolean hostnameVerificationEnabled;
        private final boolean hostnameVerificationResovleHostName;

        private ClientSSLHandler(
            final Settings settings,
            final SecureTransportSettingsProvider secureTransportSettingsProvider,
            final boolean hostnameVerificationEnabled,
            final boolean hostnameVerificationResovleHostName
        ) {
            this.settings = settings;
            this.secureTransportSettingsProvider = secureTransportSettingsProvider;
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            this.hostnameVerificationResovleHostName = hostnameVerificationResovleHostName;
        }

        @Override
        public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof DecoderException && cause != null) {
                cause = cause.getCause();
            }

            logger.error("Exception during establishing a SSL connection: " + cause, cause);

            super.exceptionCaught(ctx, cause);
        }

        @SuppressForbidden(reason = "The java.net.InetSocketAddress#getHostName() needs to be used")
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
            throws Exception {
            SSLEngine sslEngine = null;
            try {
                if (hostnameVerificationEnabled) {
                    final InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;
                    final String hostname = (hostnameVerificationResovleHostName == true)
                        ? inetSocketAddress.getHostName()
                        : inetSocketAddress.getHostString();

                    if (log.isDebugEnabled()) {
                        log.debug(
                            "Hostname of peer is {} ({}/{}) with hostnameVerificationResolveHostName: {}",
                            hostname,
                            inetSocketAddress.getHostName(),
                            inetSocketAddress.getHostString(),
                            hostnameVerificationResovleHostName
                        );
                    }

                    sslEngine = secureTransportSettingsProvider.buildSecureClientTransportEngine(
                        settings,
                        hostname,
                        inetSocketAddress.getPort()
                    ).orElse(null);

                } else {
                    sslEngine = secureTransportSettingsProvider.buildSecureClientTransportEngine(settings, null, -1).orElse(null);
                }

                if (sslEngine == null) {
                    sslEngine = SslUtils.createDefaultClientSSLEngine();
                }
            } catch (final SSLException e) {
                throw ExceptionsHelper.convertToDensityException(e);
            }

            final SslHandler sslHandler = new SslHandler(sslEngine);
            ctx.pipeline().replace(this, "ssl_client", sslHandler);
            super.connect(ctx, remoteAddress, localAddress, promise);
        }
    }

    protected class SSLClientChannelInitializer extends Netty4Transport.ClientChannelInitializer {
        private final boolean hostnameVerificationEnabled;
        private final boolean hostnameVerificationResolveHostName;
        private final DiscoveryNode node;
        private SSLConnectionTestResult connectionTestResult;

        @SuppressWarnings("removal")
        public SSLClientChannelInitializer(DiscoveryNode node) {
            this.node = node;

            final boolean dualModeEnabled = secureTransportSettingsProvider.parameters(settings)
                .map(SecureTransportSettingsProvider.SecureTransportParameters::dualModeEnabled)
                .orElse(false);
            hostnameVerificationEnabled = NetworkModule.TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION.get(settings);
            hostnameVerificationResolveHostName = NetworkModule.TRANSPORT_SSL_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME.get(settings);

            connectionTestResult = SSLConnectionTestResult.SSL_AVAILABLE;
            if (dualModeEnabled) {
                SecureConnectionTestUtil sslConnectionTestUtil = new SecureConnectionTestUtil(
                    node.getAddress().getAddress(),
                    node.getAddress().getPort()
                );
                connectionTestResult = AccessController.doPrivileged(
                    (PrivilegedAction<SSLConnectionTestResult>) sslConnectionTestUtil::testConnection
                );
            }
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);

            if (connectionTestResult == SSLConnectionTestResult.DENSITY_PING_FAILED) {
                logger.error(
                    "SSL dual mode is enabled but dual mode handshake and Density ping has failed during client connection setup, closing channel"
                );
                ch.close();
                return;
            }

            if (connectionTestResult == SSLConnectionTestResult.SSL_AVAILABLE) {
                logger.debug("Connection to {} needs to be ssl, adding ssl handler to the client channel ", node.getHostName());
                ch.pipeline()
                    .addFirst(
                        "client_ssl_handler",
                        new ClientSSLHandler(
                            settings,
                            secureTransportSettingsProvider,
                            hostnameVerificationEnabled,
                            hostnameVerificationResolveHostName
                        )
                    );
            } else {
                logger.debug("Connection to {} needs to be non ssl", node.getHostName());
            }
        }

        @Override
        public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (cause instanceof DecoderException && cause != null) {
                cause = cause.getCause();
            }

            logger.error("Exception during establishing a SSL connection: " + cause, cause);

            super.exceptionCaught(ctx, cause);
        }
    }
}
