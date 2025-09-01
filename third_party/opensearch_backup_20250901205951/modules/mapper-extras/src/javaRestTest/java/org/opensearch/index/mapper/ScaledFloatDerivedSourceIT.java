/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.mapper;

import org.density.action.DocWriteResponse;
import org.density.action.admin.indices.refresh.RefreshResponse;
import org.density.action.get.GetResponse;
import org.density.action.index.IndexRequestBuilder;
import org.density.common.settings.Settings;
import org.density.common.unit.TimeValue;
import org.density.common.xcontent.XContentType;
import org.density.core.rest.RestStatus;
import org.density.test.DensityIntegTestCase;

import java.io.IOException;

import static org.density.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.density.common.xcontent.XContentFactory.jsonBuilder;

public class ScaledFloatDerivedSourceIT extends DensityIntegTestCase {

    private static final String INDEX_NAME = "test";

    public void testScaledFloatDerivedSource() throws Exception {
        Settings.Builder settings = Settings.builder();
        settings.put(indexSettings());
        settings.put("index.derived_source.enabled", "true");

        prepareCreate(INDEX_NAME).setSettings(settings)
            .setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "scaled_float")
                    .field("scaling_factor", "100")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        ensureGreen(INDEX_NAME);

        String docId = "one_doc";
        assertEquals(DocWriteResponse.Result.CREATED, prepareIndex(docId, 1.2123422f).get().getResult());

        RefreshResponse refreshResponse = refresh(INDEX_NAME);
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
        assertEquals(0, refreshResponse.getFailedShards());
        assertEquals(INDEX_NUMBER_OF_SHARDS_SETTING.get(settings.build()).intValue(), refreshResponse.getSuccessfulShards());

        GetResponse getResponse = client().prepareGet()
            .setFetchSource(true)
            .setId(docId)
            .setIndex(INDEX_NAME)
            .get(TimeValue.timeValueMinutes(1));
        assertTrue(getResponse.isExists());
        assertEquals(1.21d, getResponse.getSourceAsMap().get("foo"));
    }

    private IndexRequestBuilder prepareIndex(String id, float number) throws IOException {
        return client().prepareIndex(INDEX_NAME)
            .setId(id)
            .setSource(jsonBuilder().startObject().field("foo", number).endObject().toString(), XContentType.JSON);
    }
}
