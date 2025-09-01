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

package org.density.search.fetch.subphase.highlight;

import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.highlight.SimpleHTMLEncoder;
import org.density.index.fieldvisitor.CustomFieldsVisitor;
import org.density.index.mapper.DerivedFieldValueFetcher;
import org.density.index.mapper.MappedFieldType;
import org.density.index.mapper.ValueFetcher;
import org.density.index.query.QueryShardContext;
import org.density.search.fetch.FetchSubPhase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singleton;

/**
 * Utility class used during the highlight phase of the search request.
 *
 * @density.internal
 */
public final class HighlightUtils {

    // U+2029 PARAGRAPH SEPARATOR (PS): each value holds a discrete passage for highlighting (unified highlighter)
    public static final char PARAGRAPH_SEPARATOR = 8233;
    public static final char NULL_SEPARATOR = '\u0000';

    private HighlightUtils() {

    }

    /**
     * Load field values for highlighting.
     */
    public static List<Object> loadFieldValues(
        MappedFieldType fieldType,
        QueryShardContext context,
        FetchSubPhase.HitContext hitContext,
        boolean forceSource
    ) throws IOException {
        if (forceSource == false && fieldType.isStored()) {
            CustomFieldsVisitor fieldVisitor = new CustomFieldsVisitor(singleton(fieldType.name()), false);
            hitContext.reader().storedFields().document(hitContext.docId(), fieldVisitor);
            List<Object> textsToHighlight = fieldVisitor.fields().get(fieldType.name());
            return textsToHighlight != null ? textsToHighlight : Collections.emptyList();
        }
        ValueFetcher fetcher = fieldType.valueFetcher(context, null, null);
        if (fetcher instanceof DerivedFieldValueFetcher) {
            fetcher.setNextReader(hitContext.reader().getContext());
        }
        return fetcher.fetchValues(hitContext.sourceLookup());
    }

    /**
     * Encoders for the highlighters
     *
     * @density.internal
     */
    public static class Encoders {
        public static final Encoder DEFAULT = new DefaultEncoder();
        public static final Encoder HTML = new SimpleHTMLEncoder();
    }

}
