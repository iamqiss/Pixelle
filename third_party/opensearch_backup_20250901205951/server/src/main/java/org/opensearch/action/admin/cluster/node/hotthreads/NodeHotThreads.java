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

package org.density.action.admin.cluster.node.hotthreads;

import org.density.action.support.nodes.BaseNodeResponse;
import org.density.cluster.node.DiscoveryNode;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Transport for Density Hot Threads
 *
 * @density.internal
 */
public class NodeHotThreads extends BaseNodeResponse {

    private String hotThreads;

    NodeHotThreads(StreamInput in) throws IOException {
        super(in);
        hotThreads = in.readString();
    }

    public NodeHotThreads(DiscoveryNode node, String hotThreads) {
        super(node);
        this.hotThreads = hotThreads;
    }

    public String getHotThreads() {
        return this.hotThreads;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(hotThreads);
    }
}
