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

package org.density.index.mapper.annotatedtext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.density.common.settings.Settings;
import org.density.index.analysis.AnalyzerScope;
import org.density.index.analysis.NamedAnalyzer;
import org.density.index.mapper.ContentPath;
import org.density.index.mapper.FieldTypeTestCase;
import org.density.index.mapper.MappedFieldType;
import org.density.index.mapper.Mapper;
import org.density.index.query.IntervalMode;

import java.io.IOException;
import java.util.Collections;

public class AnnotatedTextFieldTypeTests extends FieldTypeTestCase {

    public void testIntervals() throws IOException {
        MappedFieldType ft = new AnnotatedTextFieldMapper.AnnotatedTextFieldType("field", Collections.emptyMap());
        NamedAnalyzer a = new NamedAnalyzer("name", AnalyzerScope.INDEX, new StandardAnalyzer());
        IntervalsSource source = ft.intervals("Donald Trump", 0, IntervalMode.ORDERED, a, false);
        assertEquals(Intervals.phrase(Intervals.term("donald"), Intervals.term("trump")), source);
    }

    public void testFetchSourceValue() throws IOException {
        MappedFieldType fieldType = new AnnotatedTextFieldMapper.Builder("field", createDefaultIndexAnalyzers()).build(
            new Mapper.BuilderContext(Settings.EMPTY, new ContentPath())
        ).fieldType();

        assertEquals(Collections.singletonList("value"), fetchSourceValue(fieldType, "value"));
        assertEquals(Collections.singletonList("42"), fetchSourceValue(fieldType, 42L));
        assertEquals(Collections.singletonList("true"), fetchSourceValue(fieldType, true));
    }
}
