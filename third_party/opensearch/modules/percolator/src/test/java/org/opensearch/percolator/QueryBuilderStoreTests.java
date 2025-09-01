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

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.CheckedFunction;
import org.density.common.settings.Settings;
import org.density.core.common.io.stream.NamedWriteableRegistry;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.index.fielddata.plain.BytesBinaryIndexFieldData;
import org.density.index.mapper.BinaryFieldMapper;
import org.density.index.mapper.ContentPath;
import org.density.index.mapper.KeywordFieldMapper;
import org.density.index.mapper.Mapper;
import org.density.index.mapper.ParseContext;
import org.density.index.query.QueryShardContext;
import org.density.index.query.TermQueryBuilder;
import org.density.search.SearchModule;
import org.density.search.aggregations.support.CoreValuesSourceType;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Collections;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryBuilderStoreTests extends DensityTestCase {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testStoringQueryBuilders() throws IOException {
        try (Directory directory = newDirectory()) {
            TermQueryBuilder[] queryBuilders = new TermQueryBuilder[randomIntBetween(1, 16)];
            IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
            BinaryFieldMapper fieldMapper = PercolatorFieldMapper.Builder.createQueryBuilderFieldBuilder(
                new Mapper.BuilderContext(settings, new ContentPath(0))
            );

            Version version = Version.CURRENT;
            try (IndexWriter indexWriter = new IndexWriter(directory, config)) {
                for (int i = 0; i < queryBuilders.length; i++) {
                    queryBuilders[i] = new TermQueryBuilder(randomAlphaOfLength(4), randomAlphaOfLength(8));
                    ParseContext parseContext = mock(ParseContext.class);
                    ParseContext.Document document = new ParseContext.Document();
                    when(parseContext.doc()).thenReturn(document);
                    PercolatorFieldMapper.createQueryBuilderField(version, fieldMapper, queryBuilders[i], parseContext);
                    indexWriter.addDocument(document);
                }
            }

            QueryShardContext queryShardContext = mock(QueryShardContext.class);
            when(queryShardContext.indexVersionCreated()).thenReturn(version);
            when(queryShardContext.getWriteableRegistry()).thenReturn(writableRegistry());
            when(queryShardContext.getXContentRegistry()).thenReturn(xContentRegistry());
            when(queryShardContext.getForField(fieldMapper.fieldType())).thenReturn(
                new BytesBinaryIndexFieldData(fieldMapper.name(), CoreValuesSourceType.BYTES)
            );
            when(queryShardContext.fieldMapper(Mockito.anyString())).thenAnswer(invocation -> {
                final String fieldName = (String) invocation.getArguments()[0];
                return new KeywordFieldMapper.KeywordFieldType(fieldName);
            });
            PercolateQuery.QueryStore queryStore = PercolateQueryBuilder.createStore(fieldMapper.fieldType(), queryShardContext);

            try (IndexReader indexReader = DirectoryReader.open(directory)) {
                LeafReaderContext leafContext = indexReader.leaves().get(0);
                CheckedFunction<Integer, Query, IOException> queries = queryStore.getQueries(leafContext);
                assertEquals(queryBuilders.length, leafContext.reader().numDocs());
                for (int i = 0; i < queryBuilders.length; i++) {
                    Query query = queries.apply(i);
                    if (query instanceof ConstantScoreQuery constantScoreQuery) {
                        query = constantScoreQuery.getQuery();
                    }
                    Term term = ((TermQuery) query).getTerm();
                    assertEquals(queryBuilders[i].fieldName(), term.field());
                    assertEquals(queryBuilders[i].value(), term.text());
                }
            }
        }
    }

}
