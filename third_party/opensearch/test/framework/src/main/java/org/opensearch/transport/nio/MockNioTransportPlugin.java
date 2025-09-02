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

package org.density.transport.nio;

import org.density.Version;
import org.density.common.network.NetworkService;
import org.density.common.settings.Settings;
import org.density.common.util.PageCacheRecycler;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.indices.breaker.CircuitBreakerService;
import org.density.plugins.NetworkPlugin;
import org.density.plugins.Plugin;
import org.density.telemetry.tracing.Tracer;
import org.density.threadpool.ThreadPool;
import org.density.transport.Transport;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class MockNioTransportPlugin extends Plugin implements NetworkPlugin {

    public static final String MOCK_NIO_TRANSPORT_NAME = "mock-nio";

    @Override
    public Map<String, Supplier<Transport>> getTransports(
        Settings settings,
        ThreadPool threadPool,
        PageCacheRecycler pageCacheRecycler,
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        Tracer tracer
    ) {
        return Collections.singletonMap(
            MOCK_NIO_TRANSPORT_NAME,
            () -> new MockNioTransport(
                settings,
                Version.CURRENT,
                threadPool,
                networkService,
                pageCacheRecycler,
                namedWriteableRegistry,
                circuitBreakerService,
                tracer
            )
        );
    }
}
