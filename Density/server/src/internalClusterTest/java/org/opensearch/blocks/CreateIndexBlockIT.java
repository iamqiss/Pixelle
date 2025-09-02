/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.blocks;

import org.density.cluster.metadata.Metadata;
import org.density.common.settings.Settings;
import org.density.test.DensityIntegTestCase;
import org.junit.After;

import static org.density.test.DensityIntegTestCase.client;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertBlocked;

public class CreateIndexBlockIT extends DensityIntegTestCase {

    public void testBlockCreateIndex() {
        setCreateIndexBlock("true");
        assertBlocked(client().admin().indices().prepareCreate("uncreated-idx"), Metadata.CLUSTER_CREATE_INDEX_BLOCK);
        setCreateIndexBlock("false");
        assertAcked(client().admin().indices().prepareCreate("created-idx").execute().actionGet());
    }

    @After
    public void cleanup() throws Exception {
        Settings settings = Settings.builder().putNull(Metadata.SETTING_CREATE_INDEX_BLOCK_SETTING.getKey()).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
    }

    private void setCreateIndexBlock(String value) {
        Settings settings = Settings.builder().put(Metadata.SETTING_CREATE_INDEX_BLOCK_SETTING.getKey(), value).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
    }

}
