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

package org.density.index.analysis;

import com.ibm.icu.text.Normalizer2;
import org.apache.lucene.analysis.CharFilter;
import org.density.common.settings.Settings;
import org.density.core.index.Index;
import org.density.plugin.analysis.icu.AnalysisICUPlugin;
import org.density.test.DensityTestCase;

import java.io.StringReader;

/**
 * Test
 */
public class SimpleIcuNormalizerCharFilterTests extends DensityTestCase {
    public void testDefaultSetting() throws Exception {
        Settings settings = Settings.builder().put("index.analysis.char_filter.myNormalizerChar.type", "icu_normalizer").build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        CharFilterFactory charFilterFactory = analysis.charFilter.get("myNormalizerChar");

        String input = "ʰ㌰゙5℃№㈱㌘，バッファーの正規化のテスト．㋐㋑㋒㋓㋔ｶｷｸｹｺｻﾞｼﾞｽﾞｾﾞｿﾞg̈각/각நிเกषिchkʷक्षि";
        Normalizer2 normalizer = Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE);
        String expectedOutput = normalizer.normalize(input);
        CharFilter inputReader = (CharFilter) charFilterFactory.create(new StringReader(input));
        char[] tempBuff = new char[10];
        StringBuilder output = new StringBuilder();
        while (true) {
            int length = inputReader.read(tempBuff);
            if (length == -1) break;
            output.append(tempBuff, 0, length);
            assertEquals(output.toString(), normalizer.normalize(input.substring(0, inputReader.correctOffset(output.length()))));
        }
        assertEquals(expectedOutput, output.toString());
    }

    public void testNameAndModeSetting() throws Exception {
        Settings settings = Settings.builder()
            .put("index.analysis.char_filter.myNormalizerChar.type", "icu_normalizer")
            .put("index.analysis.char_filter.myNormalizerChar.name", "nfkc")
            .put("index.analysis.char_filter.myNormalizerChar.mode", "decompose")
            .build();
        TestAnalysis analysis = createTestAnalysis(new Index("test", "_na_"), settings, new AnalysisICUPlugin());
        CharFilterFactory charFilterFactory = analysis.charFilter.get("myNormalizerChar");

        String input = "ʰ㌰゙5℃№㈱㌘，バッファーの正規化のテスト．㋐㋑㋒㋓㋔ｶｷｸｹｺｻﾞｼﾞｽﾞｾﾞｿﾞg̈각/각நிเกषिchkʷक्षि";
        Normalizer2 normalizer = Normalizer2.getInstance(null, "nfkc", Normalizer2.Mode.DECOMPOSE);
        String expectedOutput = normalizer.normalize(input);
        CharFilter inputReader = (CharFilter) charFilterFactory.create(new StringReader(input));
        char[] tempBuff = new char[10];
        StringBuilder output = new StringBuilder();
        while (true) {
            int length = inputReader.read(tempBuff);
            if (length == -1) break;
            output.append(tempBuff, 0, length);
            assertEquals(output.toString(), normalizer.normalize(input.substring(0, inputReader.correctOffset(output.length()))));
        }
        assertEquals(expectedOutput, output.toString());
    }
}
