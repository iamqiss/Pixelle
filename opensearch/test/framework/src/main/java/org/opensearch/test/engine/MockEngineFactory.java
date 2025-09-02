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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.test.engine;

import org.apache.lucene.index.FilterDirectoryReader;
import org.density.index.engine.Engine;
import org.density.index.engine.EngineConfig;
import org.density.index.engine.EngineFactory;
import org.density.index.engine.NRTReplicationEngine;

public final class MockEngineFactory implements EngineFactory {

    private final Class<? extends FilterDirectoryReader> wrapper;

    public MockEngineFactory(Class<? extends FilterDirectoryReader> wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {

        /**
         * Segment replication enabled replicas (i.e. read only replicas) do not use an InternalEngine so a MockInternalEngine
         * will not work and an NRTReplicationEngine must be used instead. The primary shards for these indexes will
         * still use a MockInternalEngine.
         */
        return config.isReadOnlyReplica() ? new NRTReplicationEngine(config) : new MockInternalEngine(config, wrapper);
    }
}
