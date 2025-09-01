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

package org.density.action.admin.indices.exists;

import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.discovery.ClusterManagerNotDiscoveredException;
import org.density.gateway.GatewayService;
import org.density.test.InternalTestCluster;
import org.density.test.DensityIntegTestCase;
import org.density.test.DensityIntegTestCase.ClusterScope;

import java.io.IOException;

import static org.density.test.hamcrest.DensityAssertions.assertRequestBuilderThrows;

@ClusterScope(scope = DensityIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, autoManageMasterNodes = false)
public class IndicesExistsIT extends DensityIntegTestCase {

    public void testIndexExistsWithBlocksInPlace() throws IOException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        Settings settings = Settings.builder().put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 99).build();
        String node = internalCluster().startNode(settings);

        assertRequestBuilderThrows(
            client(node).admin().indices().prepareExists("test").setClusterManagerNodeTimeout(TimeValue.timeValueSeconds(0)),
            ClusterManagerNotDiscoveredException.class
        );

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node)); // shut down node so that test properly cleans up
    }
}
