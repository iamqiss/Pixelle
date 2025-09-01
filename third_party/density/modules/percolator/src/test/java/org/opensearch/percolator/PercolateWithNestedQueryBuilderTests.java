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

package org.density.percolator;

import org.density.action.admin.indices.mapping.put.PutMappingRequest;
import org.density.common.compress.CompressedXContent;
import org.density.core.common.bytes.BytesArray;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.index.mapper.MapperService;
import org.density.index.query.QueryBuilder;
import org.density.index.query.QueryShardContext;

import java.io.IOException;

public class PercolateWithNestedQueryBuilderTests extends PercolateQueryBuilderTests {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        super.initializeAdditionalMappings(mapperService);
        mapperService.merge(
            "_doc",
            new CompressedXContent(PutMappingRequest.simpleMapping("some_nested_object", "type=nested").toString()),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    public void testDetectsNestedDocuments() throws IOException {
        QueryShardContext shardContext = createShardContext();

        PercolateQueryBuilder builder = new PercolateQueryBuilder(
            queryField,
            new BytesArray("{ \"foo\": \"bar\" }"),
            MediaTypeRegistry.JSON
        );
        QueryBuilder rewrittenBuilder = rewriteAndFetch(builder, shardContext);
        PercolateQuery query = (PercolateQuery) rewrittenBuilder.toQuery(shardContext);
        assertFalse(query.excludesNestedDocs());

        builder = new PercolateQueryBuilder(
            queryField,
            new BytesArray("{ \"foo\": \"bar\", \"some_nested_object\": [ { \"baz\": 42 } ] }"),
            MediaTypeRegistry.JSON
        );
        rewrittenBuilder = rewriteAndFetch(builder, shardContext);
        query = (PercolateQuery) rewrittenBuilder.toQuery(shardContext);
        assertTrue(query.excludesNestedDocs());
    }
}
