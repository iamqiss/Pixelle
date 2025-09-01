/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.http.reactor.netty4.ssl;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.common.network.NetworkAddress;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Setting;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.common.util.MockBigArrays;
import org.density.common.util.MockPageCacheRecycler;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.common.bytes.BytesArray;
import org.density.core.common.transport.TransportAddress;
import org.density.core.common.unit.ByteSizeValue;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.http.BindHttpException;
import org.density.http.CorsHandler;
import org.density.http.HttpServerTransport;
import org.density.http.HttpTransportSettings;
import org.density.http.NullDispatcher;
import org.density.http.reactor.netty4.ReactorHttpClient;
import org.density.http.reactor.netty4.ReactorNetty4HttpServerTransport;
import org.density.plugins.SecureHttpTransportSettingsProvider;
import org.density.plugins.TransportExceptionHandler;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestRequest;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.KeyStoreUtils;
import org.density.test.DensityTestCase;
import org.density.test.rest.FakeRestRequest;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.NettyAllocator;
import org.density.transport.reactor.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import static org.density.core.rest.RestStatus.OK;
import static org.density.http.HttpTransportSettings.SETTING_CORS_ALLOW_ORIGIN;
import static org.density.http.HttpTransportSettings.SETTING_CORS_ENABLED;
import static org.density.test.KeyStoreUtils.KEYSTORE_PASSWORD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the secure {@link ReactorNetty4HttpServerTransport} class.
 */
public class SecureReactorNetty4HttpServerTransportTests extends DensityTestCase {

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private ClusterSettings clusterSettings;
    private SecureHttpTransportSettingsProvider secureHttpTransportSettingsProvider;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        var keyManagerFactory = KeyManagerFactory.getInstance("PKIX");
        keyManagerFactory.init(KeyStoreUtils.createServerKeyStore(), KEYSTORE_PASSWORD);

        secureHttpTransportSettingsProvider = new SecureHttpTransportSettingsProvider() {
            @Override
            public Optional<SecureHttpTransportParameters> parameters(Settings settings) {
                return Optional.of(new SecureHttpTransportParameters() {
                    @Override
                    public Optional<KeyManagerFactory> keyManagerFactory() {
                        return Optional.of(keyManagerFactory);
                    }

                    @Override
                    public Optional<String> sslProvider() {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<String> clientAuth() {
                        return Optional.empty();
                    }

                    @Override
                    public Collection<String> protocols() {
                        return Arrays.asList(SslUtils.DEFAULT_SSL_PROTOCOLS);
                    }

                    @Override
                    public Collection<String> cipherSuites() {
                        return Http2SecurityUtil.CIPHERS;
                    }

                    @Override
                    public Optional<TrustManagerFactory> trustManagerFactory() {
                        return Optional.of(InsecureTrustManagerFactory.INSTANCE);
                    }
                });
            }

            @Override
            public Optional<TransportExceptionHandler> buildHttpServerExceptionHandler(Settings settings, HttpServerTransport transport) {
                return Optional.empty();
            }

            @Override
            public Optional<SSLEngine> buildSecureHttpServerEngine(Settings settings, HttpServerTransport transport) throws SSLException {
                try {
                    SSLEngine engine = SslContextBuilder.forServer(keyManagerFactory)
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .build()
                        .newEngine(NettyAllocator.getAllocator());
                    return Optional.of(engine);
                } catch (final Exception ex) {
                    throw new SSLException(ex);
                }
            }
        };
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
        networkService = null;
        bigArrays = null;
        clusterSettings = null;
    }

    /**
     * Test that {@link ReactorNetty4HttpServerTransport} supports the "Expect: 100-continue" HTTP header
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeader() throws InterruptedException {
        final Settings settings = createSettings();
        final int contentLength = randomIntBetween(1, HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).bytesAsInt());
        runExpectHeaderTest(settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.CONTINUE);
    }

    /**
     * Test that {@link ReactorNetty4HttpServerTransport} responds to a
     * 100-continue expectation with too large a content-length
     * with a 413 status.
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectContinueHeaderContentLengthTooLong() throws InterruptedException {
        final String key = HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH.getKey();
        final int maxContentLength = randomIntBetween(1, 104857600);
        final Settings settings = createBuilderWithPort().put(key, maxContentLength + "b").build();
        final int contentLength = randomIntBetween(maxContentLength + 1, Integer.MAX_VALUE);
        runExpectHeaderTest(settings, HttpHeaderValues.CONTINUE.toString(), contentLength, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
    }

    /**
     * Test that {@link ReactorNetty4HttpServerTransport} responds to an unsupported expectation with a 417 status.
     * @throws InterruptedException if the client communication with the server is interrupted
     */
    public void testExpectUnsupportedExpectation() throws InterruptedException {
        Settings settings = createSettings();
        runExpectHeaderTest(settings, "chocolate=yummy", 0, HttpResponseStatus.EXPECTATION_FAILED);
    }

    private void runExpectHeaderTest(
        final Settings settings,
        final String expectation,
        final int contentLength,
        final HttpResponseStatus expectedStatus
    ) throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                channel.sendResponse(new BytesRestResponse(OK, BytesRestResponse.TEXT_CONTENT_TYPE, new BytesArray("done")));
            }

            @Override
            public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }
        };
        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            try (ReactorHttpClient client = ReactorHttpClient.https()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/");
                request.headers().set(HttpHeaderNames.EXPECT, expectation);
                HttpUtil.setContentLength(request, contentLength);

                // Reactor Netty 4 does not expose 100 CONTINUE response but instead just asks for content
                final HttpContent continuationRequest = new DefaultHttpContent(Unpooled.EMPTY_BUFFER);
                final FullHttpResponse continuationResponse = client.send(remoteAddress.address(), request, continuationRequest);
                try {
                    assertThat(continuationResponse.status(), is(HttpResponseStatus.OK));
                    assertThat(new String(ByteBufUtil.getBytes(continuationResponse.content()), StandardCharsets.UTF_8), is("done"));
                } finally {
                    continuationResponse.release();
                }
            }
        }
    }

    public void testBindUnavailableAddress() {
        Settings initialSettings = createSettings();
        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                initialSettings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                new NullDispatcher(),
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());
            Settings settings = Settings.builder()
                .put("http.port", remoteAddress.getPort())
                .put("network.host", remoteAddress.getAddress())
                .build();
            try (
                ReactorNetty4HttpServerTransport otherTransport = new ReactorNetty4HttpServerTransport(
                    settings,
                    networkService,
                    bigArrays,
                    threadPool,
                    xContentRegistry(),
                    new NullDispatcher(),
                    clusterSettings,
                    new SharedGroupFactory(settings),
                    secureHttpTransportSettingsProvider,
                    NoopTracer.INSTANCE
                )
            ) {
                BindHttpException bindHttpException = expectThrows(BindHttpException.class, otherTransport::start);
                assertEquals("Failed to bind to " + NetworkAddress.format(remoteAddress.address()), bindHttpException.getMessage());
            }
        }
    }

    public void testBadRequest() throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError();
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error("--> Unexpected bad request request");
                throw new AssertionError(cause);
            }
        };

        final Settings settings;
        final int maxInitialLineLength;
        final Setting<ByteSizeValue> httpMaxInitialLineLengthSetting = HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
        if (randomBoolean()) {
            maxInitialLineLength = httpMaxInitialLineLengthSetting.getDefault(Settings.EMPTY).bytesAsInt();
            settings = createSettings();
        } else {
            maxInitialLineLength = randomIntBetween(1, 8192);
            settings = createBuilderWithPort().put(httpMaxInitialLineLengthSetting.getKey(), maxInitialLineLength + "b").build();
        }

        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (ReactorHttpClient client = ReactorHttpClient.https()) {
                final String url = "/" + randomAlphaOfLength(maxInitialLineLength);
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);

                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.REQUEST_URI_TOO_LONG));
                    assertThat(response.content().array().length, equalTo(0));
                } finally {
                    response.release();
                }
            }
        }
    }

    public void testDispatchFailed() throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                throw new RuntimeException("Bad things happen");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error("--> Unexpected bad request request");
                throw new AssertionError(cause);
            }
        };

        final Settings settings = createSettings();
        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (ReactorHttpClient client = ReactorHttpClient.https()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");

                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.INTERNAL_SERVER_ERROR));
                    assertThat(response.content().array().length, equalTo(0));
                } finally {
                    response.release();
                }
            }
        }
    }

    public void testLargeCompressedResponse() throws InterruptedException {
        final String responseString = randomAlphaOfLength(4 * 1024 * 1024);
        final String url = "/thing/";
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                if (url.equals(request.uri())) {
                    channel.sendResponse(new BytesRestResponse(OK, responseString));
                } else {
                    logger.error("--> Unexpected successful uri [{}]", request.uri());
                    throw new AssertionError();
                }
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }

        };

        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                Settings.EMPTY,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                clusterSettings,
                new SharedGroupFactory(Settings.EMPTY),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (ReactorHttpClient client = ReactorHttpClient.https()) {
                DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
                request.headers().add(HttpHeaderNames.ACCEPT_ENCODING, randomFrom("deflate", "gzip"));
                long numOfHugeAllocations = getHugeAllocationCount();
                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(getHugeAllocationCount(), equalTo(numOfHugeAllocations));
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    byte[] bytes = new byte[response.content().readableBytes()];
                    response.content().readBytes(bytes);
                    assertThat(new String(bytes, StandardCharsets.UTF_8), equalTo(responseString));
                } finally {
                    response.release();
                }
            }
        }
    }

    private long getHugeAllocationCount() {
        long numOfHugAllocations = 0;
        ByteBufAllocator allocator = NettyAllocator.getAllocator();
        assert allocator instanceof NettyAllocator.NoDirectBuffers;
        ByteBufAllocator delegate = ((NettyAllocator.NoDirectBuffers) allocator).getDelegate();
        if (delegate instanceof PooledByteBufAllocator) {
            PooledByteBufAllocatorMetric metric = ((PooledByteBufAllocator) delegate).metric();
            numOfHugAllocations = metric.heapArenas().stream().mapToLong(PoolArenaMetric::numHugeAllocations).sum();
        }
        return numOfHugAllocations;
    }

    public void testCorsRequest() throws InterruptedException {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError();
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError();
            }

        };

        final Settings settings = createBuilderWithPort().put(SETTING_CORS_ENABLED.getKey(), true)
            .put(SETTING_CORS_ALLOW_ORIGIN.getKey(), "test-cors.org")
            .build();

        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            // Test pre-flight request
            try (ReactorHttpClient client = ReactorHttpClient.https()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.OPTIONS, "/");
                request.headers().add(CorsHandler.ORIGIN, "test-cors.org");
                request.headers().add(CorsHandler.ACCESS_CONTROL_REQUEST_METHOD, "POST");

                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    assertThat(response.headers().get(CorsHandler.ACCESS_CONTROL_ALLOW_ORIGIN), equalTo("test-cors.org"));
                    assertThat(response.headers().get(CorsHandler.VARY), equalTo(CorsHandler.ORIGIN));
                    assertTrue(response.headers().contains(CorsHandler.DATE));
                } finally {
                    response.release();
                }
            }

            // Test short-circuited request
            try (ReactorHttpClient client = ReactorHttpClient.https()) {
                final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
                request.headers().add(CorsHandler.ORIGIN, "google.com");

                final FullHttpResponse response = client.send(remoteAddress.address(), request);
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.FORBIDDEN));
                } finally {
                    response.release();
                }
            }
        }
    }

    public void testConnectTimeout() throws Exception {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                logger.error("--> Unexpected successful request [{}]", FakeRestRequest.requestToString(request));
                throw new AssertionError("Should not have received a dispatched request");
            }

            @Override
            public void dispatchBadRequest(final RestChannel channel, final ThreadContext threadContext, final Throwable cause) {
                logger.error(
                    new ParameterizedMessage("--> Unexpected bad request [{}]", FakeRestRequest.requestToString(channel.request())),
                    cause
                );
                throw new AssertionError("Should not have received a dispatched request");
            }

        };

        Settings settings = createBuilderWithPort().put(
            HttpTransportSettings.SETTING_HTTP_CONNECT_TIMEOUT.getKey(),
            new TimeValue(randomIntBetween(100, 300))
        ).build();

        NioEventLoopGroup group = new NioEventLoopGroup();
        try (
            ReactorNetty4HttpServerTransport transport = new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(settings),
                secureHttpTransportSettingsProvider,
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            final CountDownLatch channelClosedLatch = new CountDownLatch(1);

            final Bootstrap clientBootstrap = new Bootstrap().option(ChannelOption.ALLOCATOR, NettyAllocator.getAllocator())
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new ChannelHandlerAdapter() {
                        });

                    }
                })
                .group(group);
            ChannelFuture connect = clientBootstrap.connect(remoteAddress.address());
            connect.channel().closeFuture().addListener(future -> channelClosedLatch.countDown());

            assertTrue("Channel should be closed due to read timeout", channelClosedLatch.await(1, TimeUnit.MINUTES));

        } finally {
            group.shutdownGracefully().await();
        }
    }

    private Settings createSettings() {
        return createBuilderWithPort().build();
    }

    private Settings.Builder createBuilderWithPort() {
        return Settings.builder().put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange());
    }
}
