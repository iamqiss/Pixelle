/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.codec;

import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.test.DensityIntegTestCase;

import java.util.List;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST)
public class ZstdNotEnabledIT extends DensityIntegTestCase {

    public void testZStdCodecsWithoutPluginInstalled() {

        internalCluster().startNode();
        final String index = "test-index";

        // creating index with zstd and zstd_no_dict should fail if custom-codecs plugin is not installed
        for (String codec : List.of("zstd", "zstd_no_dict")) {
            assertThrows(
                IllegalArgumentException.class,
                () -> createIndex(
                    index,
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.codec", codec)
                        .build()
                )
            );
        }
    }

}
