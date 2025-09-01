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

package org.density.transport.client.node;

import org.density.common.settings.Settings;
import org.density.test.DensityIntegTestCase;
import org.density.test.DensityIntegTestCase.ClusterScope;
import org.density.test.DensityIntegTestCase.Scope;
import org.density.transport.client.Client;

import static org.hamcrest.Matchers.is;

@ClusterScope(scope = Scope.SUITE)
public class NodeClientIT extends DensityIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(Client.CLIENT_TYPE_SETTING_S.getKey(), "anything").build();
    }

    public void testThatClientTypeSettingCannotBeChanged() {
        for (Settings settings : internalCluster().getInstances(Settings.class)) {
            assertThat(Client.CLIENT_TYPE_SETTING_S.get(settings), is("node"));
        }
    }
}
