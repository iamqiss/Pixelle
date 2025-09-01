/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.http.netty4;

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
import org.density.transport.SharedGroupFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.ReferenceCounted;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class Netty4BadRequestTests extends DensityTestCase {

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
            HttpServerTransport httpServerTransport = new Netty4HttpServerTransport(
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

            try (Netty4HttpClient nettyHttpClient = Netty4HttpClient.http()) {
                final Collection<FullHttpResponse> responses = nettyHttpClient.get(
                    transportAddress.address(),
                    "/_cluster/settings?pretty=%"
                );
                try {
                    assertThat(responses, hasSize(1));
                    assertThat(responses.iterator().next().status().code(), equalTo(400));
                    final Collection<String> responseBodies = Netty4HttpClient.returnHttpResponseBodies(responses);
                    assertThat(responseBodies, hasSize(1));
                    assertThat(responseBodies.iterator().next(), containsString("\"type\":\"bad_parameter_exception\""));
                    assertThat(
                        responseBodies.iterator().next(),
                        containsString(
                            "\"reason\":\"java.lang.IllegalArgumentException: unterminated escape sequence at end of string: %\""
                        )
                    );
                } finally {
                    responses.forEach(ReferenceCounted::release);
                }
            }
        }
    }

}
