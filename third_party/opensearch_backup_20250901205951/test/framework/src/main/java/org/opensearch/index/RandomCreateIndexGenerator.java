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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.index;

import org.density.action.admin.indices.alias.Alias;
import org.density.action.admin.indices.create.CreateIndexRequest;
import org.density.common.settings.Settings;
import org.density.common.xcontent.XContentType;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.density.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.density.test.DensityTestCase.randomAlphaOfLength;
import static org.density.test.DensityTestCase.randomBoolean;
import static org.density.test.DensityTestCase.randomFrom;
import static org.density.test.DensityTestCase.randomIntBetween;

public final class RandomCreateIndexGenerator {

    private RandomCreateIndexGenerator() {}

    /**
     * Returns a random {@link CreateIndexRequest}.
     * <p>
     * Randomizes the index name, the aliases, mappings and settings associated with the
     * index. If present, the mapping definition will be nested under a type name.
     */
    public static CreateIndexRequest randomCreateIndexRequest() throws IOException {
        String index = randomAlphaOfLength(5);
        CreateIndexRequest request = new CreateIndexRequest(index);
        randomAliases(request);
        if (randomBoolean()) {
            request.mapping(randomMapping());
        }
        if (randomBoolean()) {
            request.settings(randomIndexSettings());
        }
        return request;
    }

    /**
     * Returns a {@link Settings} instance which include random values for
     * {@link org.density.cluster.metadata.IndexMetadata#SETTING_NUMBER_OF_SHARDS} and
     * {@link org.density.cluster.metadata.IndexMetadata#SETTING_NUMBER_OF_REPLICAS}
     */
    public static Settings randomIndexSettings() {
        Settings.Builder builder = Settings.builder();

        if (randomBoolean()) {
            int numberOfShards = randomIntBetween(1, 10);
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards);
        }

        if (randomBoolean()) {
            int numberOfReplicas = randomIntBetween(1, 10);
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas);
        }

        return builder.build();
    }

    /**
     * Creates a random mapping
     */
    public static XContentBuilder randomMapping() throws IOException {
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(randomFrom(XContentType.values()));
        builder.startObject();

        randomMappingFields(builder, true);

        builder.endObject();
        return builder;
    }

    /**
     * Adds random mapping fields to the provided {@link XContentBuilder}
     */
    public static void randomMappingFields(XContentBuilder builder, boolean allowObjectField) throws IOException {
        builder.startObject("properties");

        int fieldsNo = randomIntBetween(0, 5);
        Set<String> uniqueFields = new HashSet<>();
        while (uniqueFields.size() < fieldsNo) {
            uniqueFields.add(randomAlphaOfLength(5));
        }
        for (String uniqueField : uniqueFields) {
            builder.startObject(uniqueField);

            if (allowObjectField && randomBoolean()) {
                randomMappingFields(builder, false);
            } else {
                builder.field("type", "text");
            }

            builder.endObject();
        }

        builder.endObject();
    }

    /**
     * Sets random aliases to the provided {@link CreateIndexRequest}
     */
    public static void randomAliases(CreateIndexRequest request) {
        int aliasesNo = randomIntBetween(0, 2);
        for (int i = 0; i < aliasesNo; i++) {
            request.alias(randomAlias());
        }
    }

    public static Alias randomAlias() {
        Alias alias = new Alias(randomAlphaOfLength(5));

        if (randomBoolean()) {
            if (randomBoolean()) {
                alias.routing(randomAlphaOfLength(5));
            } else {
                if (randomBoolean()) {
                    alias.indexRouting(randomAlphaOfLength(5));
                }
                if (randomBoolean()) {
                    alias.searchRouting(randomAlphaOfLength(5));
                }
            }
        }

        if (randomBoolean()) {
            alias.filter("{\"term\":{\"year\":2016}}");
        }

        if (randomBoolean()) {
            alias.writeIndex(randomBoolean());
        }

        return alias;
    }
}
