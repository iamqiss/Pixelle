/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.http.reactor.netty4;

import org.density.DensityException;
import org.density.common.network.NetworkService;
import org.density.common.settings.ClusterSettings;
import org.density.common.settings.Settings;
import org.density.common.util.MockBigArrays;
import org.density.common.util.MockPageCacheRecycler;
import org.density.common.util.concurrent.ThreadContext;
import org.density.core.common.transport.TransportAddress;
import org.density.core.indices.breaker.NoneCircuitBreakerService;
import org.density.core.rest.RestStatus;
import org.density.http.HttpServerTransport;
import org.density.http.HttpTransportSettings;
import org.density.rest.BytesRestResponse;
import org.density.rest.RestChannel;
import org.density.rest.RestRequest;
import org.density.telemetry.tracing.noop.NoopTracer;
import org.density.test.DensityTestCase;
import org.density.threadpool.TestThreadPool;
import org.density.threadpool.ThreadPool;
import org.density.transport.reactor.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCounted;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ReactorNetty4BadRequestTests extends DensityTestCase {

    private NetworkService networkService;
    private MockBigArrays bigArrays;
    private ThreadPool threadPool;

    @Before
    public void setup() throws Exception {
        networkService = new NetworkService(Collections.emptyList());
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdown() throws Exception {
        terminate(threadPool);
    }

    public void testBadParameterEncoding() throws Exception {
        final HttpServerTransport.Dispatcher dispatcher = new HttpServerTransport.Dispatcher() {
            @Override
            public void dispatchRequest(RestRequest request, RestChannel channel, ThreadContext threadContext) {
                fail();
            }

            @Override
            public void dispatchBadRequest(RestChannel channel, ThreadContext threadContext, Throwable cause) {
                try {
                    final Exception e = cause instanceof Exception ? (Exception) cause : new DensityException(cause);
                    channel.sendResponse(new BytesRestResponse(channel, RestStatus.BAD_REQUEST, e));
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

        Settings settings = Settings.builder().put(HttpTransportSettings.SETTING_HTTP_PORT.getKey(), getPortRange()).build();
        try (
            HttpServerTransport httpServerTransport = new ReactorNetty4HttpServerTransport(
                settings,
                networkService,
                bigArrays,
                threadPool,
                xContentRegistry(),
                dispatcher,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                new SharedGroupFactory(Settings.EMPTY),
                NoopTracer.INSTANCE
            )
        ) {
            httpServerTransport.start();
            final TransportAddress transportAddress = randomFrom(httpServerTransport.boundAddress().boundAddresses());

            try (ReactorHttpClient nettyHttpClient = ReactorHttpClient.create()) {
                final List<FullHttpResponse> responses = nettyHttpClient.get(transportAddress.address(), "/_cluster/settings?pretty=%");

                try {
                    assertThat(responses, hasSize(1));
                    final FullHttpResponse response = responses.get(0);
                    assertThat(response.status().code(), equalTo(400));
                    final Collection<String> responseBodies = ReactorHttpClient.returnHttpResponseBodies(responses);
                    assertThat(responseBodies, hasSize(1));
                    final String body = responseBodies.iterator().next();
                    assertThat(body, containsString("\"type\":\"bad_parameter_exception\""));
                    assertThat(
                        body,
                        containsString("\"reason\":\"java.lang.IllegalArgumentException: partial escape sequence at end of string: %/\"")
                    );
                } finally {
                    responses.forEach(ReferenceCounted::release);
                }
            }
        }
    }

}
