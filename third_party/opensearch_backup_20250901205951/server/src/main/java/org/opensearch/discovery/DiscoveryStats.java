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

package org.density.discovery;

import org.density.Version;
import org.density.cluster.coordination.PendingClusterStateStats;
import org.density.cluster.coordination.PublishClusterStateStats;
import org.density.cluster.service.ClusterStateStats;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.StreamOutput;
import org.density.core.common.io.stream.Writeable;
import org.density.core.xcontent.ToXContentFragment;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stores discovery stats
 *
 * @density.internal
 */
public class DiscoveryStats implements Writeable, ToXContentFragment {

    private final PendingClusterStateStats queueStats;
    private final PublishClusterStateStats publishStats;
    private final ClusterStateStats clusterStateStats;

    public DiscoveryStats(PendingClusterStateStats queueStats, PublishClusterStateStats publishStats, ClusterStateStats clusterStateStats) {
        this.queueStats = queueStats;
        this.publishStats = publishStats;
        this.clusterStateStats = clusterStateStats;
    }

    public DiscoveryStats(StreamInput in) throws IOException {
        queueStats = in.readOptionalWriteable(PendingClusterStateStats::new);
        publishStats = in.readOptionalWriteable(PublishClusterStateStats::new);
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            clusterStateStats = in.readOptionalWriteable(ClusterStateStats::new);
        } else {
            clusterStateStats = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(queueStats);
        out.writeOptionalWriteable(publishStats);
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(clusterStateStats);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DISCOVERY);
        if (queueStats != null) {
            queueStats.toXContent(builder, params);
        }
        if (publishStats != null) {
            publishStats.toXContent(builder, params);
        }
        if (clusterStateStats != null) {
            clusterStateStats.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String DISCOVERY = "discovery";
    }

    public PendingClusterStateStats getQueueStats() {
        return queueStats;
    }

    public PublishClusterStateStats getPublishStats() {
        return publishStats;
    }

    public ClusterStateStats getClusterStateStats() {
        return clusterStateStats;
    }
}
