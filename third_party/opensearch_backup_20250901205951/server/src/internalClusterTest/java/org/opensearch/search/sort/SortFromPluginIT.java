/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.sort;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.density.action.search.SearchResponse;
import org.density.common.settings.Settings;
import org.density.common.xcontent.json.JsonXContent;
import org.density.plugins.Plugin;
import org.density.search.builder.SearchSourceBuilder;
import org.density.search.sort.plugin.CustomSortBuilder;
import org.density.search.sort.plugin.CustomSortPlugin;
import org.density.test.InternalSettingsPlugin;
import org.density.test.ParameterizedStaticSettingsDensityIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.density.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class SortFromPluginIT extends ParameterizedStaticSettingsDensityIntegTestCase {
    public SortFromPluginIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomSortPlugin.class, InternalSettingsPlugin.class);
    }

    public void testPluginSort() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", 2).get();
        client().prepareIndex("test").setId("2").setSource("field", 1).get();
        client().prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();
        indexRandomForConcurrentSearch("test");

        SearchResponse searchResponse = client().prepareSearch("test").addSort(new CustomSortBuilder("field", SortOrder.ASC)).get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test").addSort(new CustomSortBuilder("field", SortOrder.DESC)).get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testPluginSortXContent() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", 2).get();
        client().prepareIndex("test").setId("2").setSource("field", 1).get();
        client().prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();
        indexRandomForConcurrentSearch("test");

        // builder -> json -> builder
        SearchResponse searchResponse = client().prepareSearch("test")
            .setSource(
                SearchSourceBuilder.fromXContent(
                    createParser(
                        JsonXContent.jsonXContent,
                        new SearchSourceBuilder().sort(new CustomSortBuilder("field", SortOrder.ASC)).toString()
                    )
                )
            )
            .get();

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
            .setSource(
                SearchSourceBuilder.fromXContent(
                    createParser(
                        JsonXContent.jsonXContent,
                        new SearchSourceBuilder().sort(new CustomSortBuilder("field", SortOrder.DESC)).toString()
                    )
                )
            )
            .get();

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }
}
