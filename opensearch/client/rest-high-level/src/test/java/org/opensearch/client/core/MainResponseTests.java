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

package org.density.client.core;

import org.density.Build;
import org.density.Version;
import org.density.client.AbstractResponseTestCase;
import org.density.cluster.ClusterName;
import org.density.common.xcontent.XContentType;
import org.density.core.xcontent.XContentParser;
import org.density.test.VersionUtils;

import java.io.IOException;
import java.util.Date;

import static org.hamcrest.Matchers.equalTo;

public class MainResponseTests extends AbstractResponseTestCase<org.density.action.main.MainResponse, MainResponse> {
    private static String DISTRIBUTION = "density";

    @Override
    protected org.density.action.main.MainResponse createServerTestInstance(XContentType xContentType) {
        String clusterUuid = randomAlphaOfLength(10);
        ClusterName clusterName = new ClusterName(randomAlphaOfLength(10));
        String nodeName = randomAlphaOfLength(10);
        final String date = new Date(randomNonNegativeLong()).toString();
        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Build build = new Build(Build.Type.UNKNOWN, randomAlphaOfLength(8), date, randomBoolean(), version.toString(), DISTRIBUTION);
        return new org.density.action.main.MainResponse(nodeName, version, clusterName, clusterUuid, build);
    }

    @Override
    protected MainResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return MainResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.density.action.main.MainResponse serverTestInstance, MainResponse clientInstance) {
        assertThat(serverTestInstance.getClusterName().value(), equalTo(clientInstance.getClusterName()));
        assertThat(serverTestInstance.getClusterUuid(), equalTo(clientInstance.getClusterUuid()));
        assertThat(serverTestInstance.getNodeName(), equalTo(clientInstance.getNodeName()));

        assertThat(serverTestInstance.getBuild().hash(), equalTo(clientInstance.getVersion().getBuildHash()));
        assertThat(serverTestInstance.getVersion().toString(), equalTo(clientInstance.getVersion().getNumber()));
        assertThat(serverTestInstance.getBuild().date(), equalTo(clientInstance.getVersion().getBuildDate()));
        assertThat(serverTestInstance.getBuild().type().displayName(), equalTo(clientInstance.getVersion().getBuildType()));
        assertThat(serverTestInstance.getVersion().luceneVersion.toString(), equalTo(clientInstance.getVersion().getLuceneVersion()));
        assertThat(
            serverTestInstance.getVersion().minimumIndexCompatibilityVersion().toString(),
            equalTo(clientInstance.getVersion().getMinimumIndexCompatibilityVersion())
        );
        assertThat(
            serverTestInstance.getVersion().minimumCompatibilityVersion().toString(),
            equalTo(clientInstance.getVersion().getMinimumWireCompatibilityVersion())
        );
    }
}
