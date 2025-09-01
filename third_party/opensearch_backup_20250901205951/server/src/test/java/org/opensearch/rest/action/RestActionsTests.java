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

package org.density.rest.action;

import com.fasterxml.jackson.core.io.JsonEOFException;

import org.density.action.OriginalIndices;
import org.density.action.search.ShardSearchFailure;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.common.xcontent.json.JsonXContent;
import org.density.core.action.ShardOperationFailedException;
import org.density.core.common.ParsingException;
import org.density.core.index.Index;
import org.density.core.index.shard.ShardId;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.core.xcontent.XContentParser;
import org.density.index.query.MatchQueryBuilder;
import org.density.index.query.QueryBuilder;
import org.density.search.SearchModule;
import org.density.search.SearchShardTarget;
import org.density.test.DensityTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.equalTo;

public class RestActionsTests extends DensityTestCase {

    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }

    @AfterClass
    public static void cleanup() {
        xContentRegistry = null;
    }

    public void testParseTopLevelBuilder() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("foo", "bar");
        String requestBody = "{ \"query\" : " + query.toString() + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
            QueryBuilder actual = RestActions.getQueryContent(parser);
            assertEquals(query, actual);
        }
    }

    public void testParseTopLevelBuilderEmptyObject() throws IOException {
        for (String requestBody : Arrays.asList("{}", "")) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
                QueryBuilder query = RestActions.getQueryContent(parser);
                assertNull(query);
            }
        }
    }

    public void testParseTopLevelBuilderMalformedJson() throws IOException {
        for (String requestBody : Arrays.asList("\"\"", "\"someString\"", "\"{\"")) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
                ParsingException exception = expectThrows(ParsingException.class, () -> RestActions.getQueryContent(parser));
                assertEquals("Expected [START_OBJECT] but found [VALUE_STRING]", exception.getMessage());
            }
        }
    }

    public void testParseTopLevelBuilderIncompleteJson() throws IOException {
        for (String requestBody : Arrays.asList("{", "{ \"query\" :")) {
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
                ParsingException exception = expectThrows(ParsingException.class, () -> RestActions.getQueryContent(parser));
                assertEquals("Failed to parse", exception.getMessage());
                assertEquals(JsonEOFException.class, exception.getRootCause().getClass());
            }
        }
    }

    public void testParseTopLevelBuilderUnknownParameter() throws IOException {
        String requestBody = "{ \"foo\" : \"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, requestBody)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> RestActions.getQueryContent(parser));
            assertEquals("request does not support [foo]", exception.getMessage());
        }
    }

    public void testBuildBroadcastShardsHeader() throws IOException {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] {
            createShardFailureParsingException("node0", 0, null),
            createShardFailureParsingException("node1", 1, null),
            createShardFailureParsingException("node2", 2, null),
            createShardFailureParsingException("node0", 0, "cluster1"),
            createShardFailureParsingException("node1", 1, "cluster1"),
            createShardFailureParsingException("node2", 2, "cluster1"),
            createShardFailureParsingException("node0", 0, "cluster2"),
            createShardFailureParsingException("node1", 1, "cluster2"),
            createShardFailureParsingException("node2", 2, "cluster2") };

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.prettyPrint();
        builder.startObject();
        RestActions.buildBroadcastShardsHeader(builder, ToXContent.EMPTY_PARAMS, 12, 3, 0, 9, failures);
        builder.endObject();
        assertThat(
            builder.toString(),
            equalTo(
                "{\n"
                    + "  \"_shards\" : {\n"
                    + "    \"total\" : 12,\n"
                    + "    \"successful\" : 3,\n"
                    + "    \"skipped\" : 0,\n"
                    + "    \"failed\" : 9,\n"
                    + "    \"failures\" : [\n"
                    + "      {\n"
                    + "        \"shard\" : 0,\n"
                    + "        \"index\" : \"index\",\n"
                    + "        \"node\" : \"node0\",\n"
                    + "        \"reason\" : {\n"
                    + "          \"type\" : \"parsing_exception\",\n"
                    + "          \"reason\" : \"error\",\n"
                    + "          \"index\" : \"index\",\n"
                    + "          \"index_uuid\" : \"_na_\",\n"
                    + "          \"line\" : 0,\n"
                    + "          \"col\" : 0,\n"
                    + "          \"caused_by\" : {\n"
                    + "            \"type\" : \"illegal_argument_exception\",\n"
                    + "            \"reason\" : \"some bad argument\"\n"
                    + "          }\n"
                    + "        }\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"shard\" : 0,\n"
                    + "        \"index\" : \"cluster1:index\",\n"
                    + "        \"node\" : \"node0\",\n"
                    + "        \"reason\" : {\n"
                    + "          \"type\" : \"parsing_exception\",\n"
                    + "          \"reason\" : \"error\",\n"
                    + "          \"index\" : \"index\",\n"
                    + "          \"index_uuid\" : \"_na_\",\n"
                    + "          \"line\" : 0,\n"
                    + "          \"col\" : 0,\n"
                    + "          \"caused_by\" : {\n"
                    + "            \"type\" : \"illegal_argument_exception\",\n"
                    + "            \"reason\" : \"some bad argument\"\n"
                    + "          }\n"
                    + "        }\n"
                    + "      },\n"
                    + "      {\n"
                    + "        \"shard\" : 0,\n"
                    + "        \"index\" : \"cluster2:index\",\n"
                    + "        \"node\" : \"node0\",\n"
                    + "        \"reason\" : {\n"
                    + "          \"type\" : \"parsing_exception\",\n"
                    + "          \"reason\" : \"error\",\n"
                    + "          \"index\" : \"index\",\n"
                    + "          \"index_uuid\" : \"_na_\",\n"
                    + "          \"line\" : 0,\n"
                    + "          \"col\" : 0,\n"
                    + "          \"caused_by\" : {\n"
                    + "            \"type\" : \"illegal_argument_exception\",\n"
                    + "            \"reason\" : \"some bad argument\"\n"
                    + "          }\n"
                    + "        }\n"
                    + "      }\n"
                    + "    ]\n"
                    + "  }\n"
                    + "}"
            )
        );
    }

    private static ShardSearchFailure createShardFailureParsingException(String nodeId, int shardId, String clusterAlias) {
        String index = "index";
        ParsingException ex = new ParsingException(0, 0, "error", new IllegalArgumentException("some bad argument"));
        ex.setIndex(index);
        return new ShardSearchFailure(ex, createSearchShardTarget(nodeId, shardId, index, clusterAlias));
    }

    private static SearchShardTarget createSearchShardTarget(String nodeId, int shardId, String index, String clusterAlias) {
        return new SearchShardTarget(
            nodeId,
            new ShardId(new Index(index, IndexMetadata.INDEX_UUID_NA_VALUE), shardId),
            clusterAlias,
            OriginalIndices.NONE
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
