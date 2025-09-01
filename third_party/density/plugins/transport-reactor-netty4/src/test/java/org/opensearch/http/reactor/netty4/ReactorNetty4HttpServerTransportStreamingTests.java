/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.http.reactor.netty4;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.density.common.lease.Releasable;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.MockBigArrays;
import org.density.common.util.MockPageCacheRecycler;
import org.density.common.util.concurrent.ThreadContext;
import org.density.common.xcontent.XContentType;
import org.density.common.xcontent.support.XContentHttpChunk;
import org.density.core.common.transport.TransportAddress;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.http.HttpServerTransport;
import org.density.rest.RestChannel;
import org.density.rest.RestHandler;
import org.density.rest.RestRequest;
import org.density.rest.RestRequest.Method;
import org.density.rest.StreamingRestChannel;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.test.rest.FakeRestRequest;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.client.node.NodeClient;
import org.density.transport.reactor.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the {@link ReactorNetty4HttpServerTransport} class with streaming support.
 */
public class ReactorNetty4HttpServerTransportStreamingTests extends DensityTestCase {
    private static final Function<String, ToXContent> XCONTENT_CONVERTER = (str) -> new ToXContent() {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            return builder.startObject().field("doc", str).endObject();
        }
    };

    private NetworkService networkService;
    private ThreadPool threadPool;
    private MockBigArrays bigArrays;
    private ClusterSettings clusterSettings;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        threadPool = new TestThreadPool("test");
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
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

    public void testRequestResponseStreaming() throws InterruptedException {
        final String responseString = randomAlphaOfLength(4 * 1024);
        final String url = "/stream/";

        final ToXContent[] chunks = newChunks(responseString);
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public Optional<RestHandler> dispatchHandler(String uri, String rawPath, Method method, Map<String, String> params) {
                return Optional.of(new RestHandler() {
                    @Override
                    public boolean supportsStreaming() {
                        return true;
                    }

                    @Override
                    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
                        logger.error("--> Unexpected request [{}]", request.uri());
                        throw new AssertionError();
                    }
                });
            }

            @Override
            public void dispatchRequest(final RestRequest request, final RestChannel channel, final ThreadContext threadContext) {
                if (url.equals(request.uri())) {
                    assertThat(channel, instanceOf(StreamingRestChannel.class));
                    final StreamingRestChannel streamingChannel = (StreamingRestChannel) channel;

                    // Await at most 5 seconds till channel is ready for writing the response stream, fail otherwise
                    final Mono<?> ready = Mono.fromRunnable(() -> {
                        while (!streamingChannel.isWritable()) {
                            Thread.onSpinWait();
                        }
                    }).timeout(Duration.ofSeconds(5));

                    threadPool.executor(ThreadPool.Names.WRITE)
                        .execute(() -> Flux.concat(Flux.fromArray(newChunks(responseString)).map(e -> {
                            try (XContentBuilder builder = channel.newBuilder(XContentType.JSON, true)) {
                                return XContentHttpChunk.from(e.toXContent(builder, ToXContent.EMPTY_PARAMS));
                            } catch (final IOException ex) {
                                throw new UncheckedIOException(ex);
                            }
                        }), Mono.just(XContentHttpChunk.last()))
                            .delaySubscription(ready)
                            .subscribe(streamingChannel::sendChunk, null, () -> {
                                if (channel.bytesOutput() instanceof Releasable) {
                                    ((Releasable) channel.bytesOutput()).close();
                                }
                            }));
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
                NoopTracer.INSTANCE
            )
        ) {
            transport.start();
            final TransportAddress remoteAddress = randomFrom(transport.boundAddress().boundAddresses());

            try (ReactorHttpClient client = ReactorHttpClient.create(false)) {
                HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
                final FullHttpResponse response = client.stream(remoteAddress.address(), request, Arrays.stream(chunks));
                try {
                    assertThat(response.status(), equalTo(HttpResponseStatus.OK));
                    byte[] bytes = new byte[response.content().readableBytes()];
                    response.content().readBytes(bytes);
                    assertThat(new String(bytes, StandardCharsets.UTF_8), equalTo(Arrays.stream(newChunks(responseString)).map(s -> {
                        try (XContentBuilder builder = XContentType.JSON.contentBuilder()) {
                            return s.toXContent(builder, ToXContent.EMPTY_PARAMS).toString();
                        } catch (final IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    }).collect(Collectors.joining("\r\n", "", "\r\n"))));
                } finally {
                    response.release();
                }
            }
        }
    }

    private static ToXContent[] newChunks(final String responseString) {
        final ToXContent[] chunks = new ToXContent[responseString.length() / 16];

        for (int chunk = 0; chunk < responseString.length(); chunk += 16) {
            chunks[chunk / 16] = XCONTENT_CONVERTER.apply(responseString.substring(chunk, chunk + 16));
        }

        return chunks;
    }
}
