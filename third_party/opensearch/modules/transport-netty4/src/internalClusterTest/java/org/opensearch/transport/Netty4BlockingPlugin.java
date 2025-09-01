/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport;

import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.BigArrays;
import org.density.common.util.PageCacheRecycler;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.http.HttpServerTransport;
import org.density.http.netty4.Netty4HttpServerTransport;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;

public class Netty4BlockingPlugin extends Netty4ModulePlugin {

    public class Netty4BlockingHttpServerTransport extends Netty4HttpServerTransport {

        public Netty4BlockingHttpServerTransport(
            Settings settings,
            NetworkService networkService,
            BigArrays bigArrays,
            ThreadPool threadPool,
            NamedXContentRegistry xContentRegistry,
            Dispatcher dispatcher,
            ClusterSettings clusterSettings,
            SharedGroupFactory sharedGroupFactory,
            Tracer tracer
        ) {
            super(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                sharedGroupFactory,
                tracer
            );
        }

        @Override
        protected ChannelInboundHandlerAdapter createHeaderVerifier() {
            return new ExampleBlockingNetty4HeaderVerifier();
        }
    }

    @Override
    public Map<String, Supplier<HttpServerTransport>> getHttpTransports(
        Settings settings,
        ThreadPool threadPool,
        BigArrays bigArrays,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedXContentRegistry xContentRegistry,
        NetworkService networkService,
        HttpServerTransport.Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        return Collections.singletonMap(
            NETTY_HTTP_TRANSPORT_NAME,
            () -> new Netty4BlockingHttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry,
                dispatcher,
                clusterSettings,
                getSharedGroupFactory(settings),
                tracer
            )
        );
    }

    /** POC for how an external header verifier would be implemented */
    public class ExampleBlockingNetty4HeaderVerifier extends SimpleChannelInboundHandler<DefaultHttpRequest> {

        @Override
        public void channelRead0(ChannelHandlerContext ctx, DefaultHttpRequest msg) throws Exception {
            ReferenceCountUtil.retain(msg);
            if (isBlocked(msg)) {
                ByteBuf buf = Unpooled.copiedBuffer("Hit header_verifier".getBytes(StandardCharsets.UTF_8));
                final FullHttpResponse response = new DefaultFullHttpResponse(msg.protocolVersion(), HttpResponseStatus.UNAUTHORIZED, buf);
                ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
                ReferenceCountUtil.release(msg);
            } else {
                // Lets the request pass to the next channel handler
                ctx.fireChannelRead(msg);
            }
        }

        private boolean isBlocked(HttpRequest request) {
            final boolean shouldBlock = request.headers().contains("blockme");

            return shouldBlock;
        }
    }
}
