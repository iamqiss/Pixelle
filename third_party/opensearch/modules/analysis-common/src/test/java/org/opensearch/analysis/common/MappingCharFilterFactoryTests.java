/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.analysis.common;

import org.apache.lucene.analysis.CharFilter;
import org.density.common.settings.Settings;
import org.density.env.Environment;
import org.density.index.analysis.AnalysisTestsHelper;
import org.density.index.analysis.CharFilterFactory;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;

public class MappingCharFilterFactoryTests extends DensityTestCase {
    public static CharFilterFactory create(String... rules) throws IOException {
        DensityTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
                .put("index.analysis.analyzer.my_analyzer.char_filter", "my_mappings_char_filter")
                .put("index.analysis.char_filter.my_mappings_char_filter.type", "mapping")
                .putList("index.analysis.char_filter.my_mappings_char_filter.mappings", rules)
                .build(),
            new CommonAnalysisModulePlugin()
        );

        return analysis.charFilter.get("my_mappings_char_filter");
    }

    public void testRulesOk() throws IOException {
        MappingCharFilterFactory mappingCharFilterFactory = (MappingCharFilterFactory) create(
            "# This is a comment",
            "# => _hashtag_",
            ":) => _happy_",
            ":( => _sad_"
        );
        CharFilter inputReader = (CharFilter) mappingCharFilterFactory.create(new StringReader("I'm so :), I'm so :( #confused"));
        char[] tempBuff = new char[14];
        StringBuilder output = new StringBuilder();
        while (true) {
            int length = inputReader.read(tempBuff);
            if (length == -1) break;
            output.append(tempBuff, 0, length);
        }
        assertEquals("I'm so _happy_, I'm so _sad_ _hashtag_confused", output.toString());
    }

    public void testRuleError() {
        for (String rule : Arrays.asList(
            "",        // empty
            "a",       // no arrow
            "a:>b"     // invalid delimiter
        )) {
            RuntimeException ex = expectThrows(RuntimeException.class, () -> create(rule));
            assertEquals("Line [1]: Invalid mapping rule : [" + rule + "]", ex.getMessage());
        }
    }

    public void testRulePartError() {
        RuntimeException ex = expectThrows(
            RuntimeException.class,
            () -> create("# This is a comment", "# => _hashtag_", ":) => _happy_", "a:b")
        );
        assertEquals("Line [4]: Invalid mapping rule : [a:b]", ex.getMessage());
    }
}
