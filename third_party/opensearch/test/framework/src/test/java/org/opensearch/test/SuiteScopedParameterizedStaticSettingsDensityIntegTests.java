/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.test;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.admin.cluster.node.info.NodeInfo;
import org.density.action.admin.cluster.node.info.NodesInfoResponse;
import org.density.common.settings.Settings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.hamcrest.CoreMatchers.equalTo;

@DensityIntegTestCase.SuiteScopeTestCase
public class SuiteScopedParameterizedStaticSettingsDensityIntegTests extends ParameterizedStaticSettingsDensityIntegTestCase {
    public SuiteScopedParameterizedStaticSettingsDensityIntegTests(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public void testSettings() throws IOException {
        final NodesInfoResponse nodes = client().admin().cluster().prepareNodesInfo().get();
        for (final NodeInfo node : nodes.getNodes()) {
            assertThat(
                CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.get(node.getSettings()),
                equalTo(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.get(settings))
            );
        }
    }
}
