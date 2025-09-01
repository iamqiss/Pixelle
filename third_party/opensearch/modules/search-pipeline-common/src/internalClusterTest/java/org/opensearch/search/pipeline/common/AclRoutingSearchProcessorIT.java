/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.search.pipeline.common;

import org.density.action.admin.indices.create.CreateIndexRequest;
import org.density.action.index.IndexRequest;
import org.density.action.search.PutSearchPipelineRequest;
import org.density.action.search.SearchRequest;
import org.density.action.search.SearchResponse;
import org.density.action.support.clustermanager.AcknowledgedResponse;
import org.density.common.hash.MurmurHash3;
import org.density.common.settings.Settings;
import org.density.core.common.bytes.BytesArray;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.query.QueryBuilders;
import org.density.plugins.Plugin;
import org.density.search.builder.SearchSourceBuilder;
import org.density.test.DensityIntegTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.density.common.xcontent.XContentFactory.jsonBuilder;
import static org.density.test.hamcrest.DensityAssertions.assertAcked;
import static org.density.test.hamcrest.DensityAssertions.assertHitCount;

@DensityIntegTestCase.ClusterScope(scope = DensityIntegTestCase.Scope.TEST)
public class AclRoutingSearchProcessorIT extends DensityIntegTestCase {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SearchPipelineCommonModulePlugin.class);
    }

    public void testSearchProcessorExtractsRouting() throws Exception {
        // Create search pipeline first
        String pipelineId = "acl-search-pipeline";
        BytesArray pipelineConfig = new BytesArray("""
            {
              "request_processors": [
                {
                  "acl_routing_search": {
                    "acl_field": "team",
                    "extract_from_query": true
                  }
                }
              ]
            }
            """);

        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(pipelineId, pipelineConfig, MediaTypeRegistry.JSON);
        AcknowledgedResponse putResponse = client().admin().cluster().putSearchPipeline(putRequest).actionGet();
        assertTrue("Pipeline creation should succeed", putResponse.isAcknowledged());

        // Create index with multiple shards
        String indexName = "test-acl-search-routing";
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName).settings(
            Settings.builder().put("number_of_shards", 2).put("number_of_replicas", 0).build()
        )
            .mapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("team")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("content")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
            );

        client().admin().indices().create(createIndexRequest).get();

        // Index test documents with explicit routing
        String team1 = "team-alpha";
        String team2 = "team-beta";
        String team1Routing = generateRoutingValue(team1);
        String team2Routing = generateRoutingValue(team2);

        // Alpha team documents
        client().index(
            new IndexRequest(indexName).id("1")
                .routing(team1Routing)
                .source(jsonBuilder().startObject().field("team", team1).field("content", "alpha content 1").endObject())
        ).get();

        client().index(
            new IndexRequest(indexName).id("2")
                .routing(team1Routing)
                .source(jsonBuilder().startObject().field("team", team1).field("content", "alpha content 2").endObject())
        ).get();

        // Beta team document
        client().index(
            new IndexRequest(indexName).id("3")
                .routing(team2Routing)
                .source(jsonBuilder().startObject().field("team", team2).field("content", "beta content").endObject())
        ).get();

        client().admin().indices().prepareRefresh(indexName).get();

        // Test search with pipeline - should find only alpha team docs
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("team", team1)));
        searchRequest.pipeline(pipelineId);

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 2);

        // Verify all results are from team alpha
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            Map<String, Object> source = searchResponse.getHits().getAt(i).getSourceAsMap();
            assertEquals("All documents should be from team alpha", team1, source.get("team"));
        }
    }

    public void testSearchProcessorWithBoolQuery() throws Exception {
        String indexName = "test-search-bool-query";

        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", 2)));

        String pipelineId = "acl-bool-search-pipeline";
        BytesArray pipelineConfig = new BytesArray(
            "{\n"
                + "  \"request_processors\": [\n"
                + "    {\n"
                + "      \"acl_routing_search\": {\n"
                + "        \"acl_field\": \"department\",\n"
                + "        \"extract_from_query\": true\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}"
        );
        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(pipelineId, pipelineConfig, MediaTypeRegistry.JSON);
        AcknowledgedResponse putResponse = client().admin().cluster().putSearchPipeline(putRequest).actionGet();
        assertTrue("Pipeline creation should succeed", putResponse.isAcknowledged());

        // Index documents
        String dept = "engineering";
        String deptRouting = generateRoutingValue(dept);
        for (int i = 0; i < 2; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("department", dept);
            doc.put("title", "Engineer " + i);

            IndexRequest indexRequest = new IndexRequest(indexName).id("eng-doc-" + i).source(doc).routing(deptRouting);

            client().index(indexRequest).get();
        }

        client().admin().indices().prepareRefresh(indexName).get();

        // Search with bool query
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("department", dept)).filter(QueryBuilders.existsQuery("title"))
            )
        );
        searchRequest.pipeline(pipelineId);

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 2);
    }

    public void testSearchProcessorWithoutAclInQuery() throws Exception {
        String indexName = "test-search-no-acl";

        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", 2)));

        String pipelineId = "acl-no-match-pipeline";
        BytesArray pipelineConfig = new BytesArray(
            "{\n"
                + "  \"request_processors\": [\n"
                + "    {\n"
                + "      \"acl_routing_search\": {\n"
                + "        \"acl_field\": \"team\",\n"
                + "        \"extract_from_query\": true\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}"
        );
        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(pipelineId, pipelineConfig, MediaTypeRegistry.JSON);
        AcknowledgedResponse putResponse = client().admin().cluster().putSearchPipeline(putRequest).actionGet();
        assertTrue("Pipeline creation should succeed", putResponse.isAcknowledged());

        // Index a document
        Map<String, Object> doc = new HashMap<>();
        doc.put("content", "some content");

        IndexRequest indexRequest = new IndexRequest(indexName).id("doc-1").source(doc);

        client().index(indexRequest).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Search without team filter
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()));
        searchRequest.pipeline(pipelineId);

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 1);
    }

    public void testSearchProcessorDisabled() throws Exception {
        String indexName = "test-search-disabled";

        assertAcked(prepareCreate(indexName).setSettings(Settings.builder().put("number_of_shards", 2)));

        String pipelineId = "acl-disabled-pipeline";
        BytesArray pipelineConfig = new BytesArray(
            "{\n"
                + "  \"request_processors\": [\n"
                + "    {\n"
                + "      \"acl_routing_search\": {\n"
                + "        \"acl_field\": \"team\",\n"
                + "        \"extract_from_query\": false\n"
                + "      }\n"
                + "    }\n"
                + "  ]\n"
                + "}"
        );
        PutSearchPipelineRequest putRequest = new PutSearchPipelineRequest(pipelineId, pipelineConfig, MediaTypeRegistry.JSON);
        AcknowledgedResponse putResponse = client().admin().cluster().putSearchPipeline(putRequest).actionGet();
        assertTrue("Pipeline creation should succeed", putResponse.isAcknowledged());

        // Index a document
        String team = "engineering";
        Map<String, Object> doc = new HashMap<>();
        doc.put("team", team);
        doc.put("content", "engineering content");

        IndexRequest indexRequest = new IndexRequest(indexName).id("doc-1").source(doc).routing(generateRoutingValue(team));

        client().index(indexRequest).get();
        client().admin().indices().prepareRefresh(indexName).get();

        // Search with team filter but extraction disabled
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(new SearchSourceBuilder().query(QueryBuilders.termQuery("team", "engineering")));
        searchRequest.pipeline(pipelineId);

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 1);
    }

    private String generateRoutingValue(String aclValue) {
        // Use MurmurHash3 for consistent hashing (same as processors)
        byte[] bytes = aclValue.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());

        // Convert to base64 for routing value
        byte[] hashBytes = new byte[16];
        System.arraycopy(longToBytes(hash.h1), 0, hashBytes, 0, 8);
        System.arraycopy(longToBytes(hash.h2), 0, hashBytes, 8, 8);

        return BASE64_ENCODER.encodeToString(hashBytes);
    }

    private byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }
}
