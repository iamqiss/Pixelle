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

package org.density.analysis.common;

import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.env.Environment;
import org.density.index.IndexSettings;
import org.density.index.analysis.IndexAnalyzers;
import org.density.index.analysis.NamedAnalyzer;
import org.density.test.IndexSettingsModule;
import org.density.test.DensityTokenStreamTestCase;

import static org.density.test.DensityTestCase.createTestAnalysis;
import static org.hamcrest.Matchers.containsString;

public class PatternCaptureTokenFilterTests extends DensityTokenStreamTestCase {
    public void testPatternCaptureTokenFilter() throws Exception {
        String json = "/org/density/analysis/common/pattern_capture.json";
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        IndexAnalyzers indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisModulePlugin()).indexAnalyzers;
        NamedAnalyzer analyzer1 = indexAnalyzers.get("single");

        assertTokenStreamContents(analyzer1.tokenStream("test", "foobarbaz"), new String[] { "foobarbaz", "foobar", "foo" });

        NamedAnalyzer analyzer2 = indexAnalyzers.get("multi");

        assertTokenStreamContents(analyzer2.tokenStream("test", "abc123def"), new String[] { "abc123def", "abc", "123", "def" });

        NamedAnalyzer analyzer3 = indexAnalyzers.get("preserve");

        assertTokenStreamContents(analyzer3.tokenStream("test", "foobarbaz"), new String[] { "foobar", "foo" });
    }

    public void testNoPatterns() {
        try {
            new PatternCaptureGroupTokenFilterFactory(
                IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
                null,
                "pattern_capture",
                Settings.builder().put("pattern", "foobar").build()
            );
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("required setting 'patterns' is missing"));
        }
    }

}
