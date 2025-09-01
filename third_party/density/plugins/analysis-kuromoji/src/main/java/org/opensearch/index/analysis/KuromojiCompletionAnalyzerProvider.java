/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.analysis;

import org.apache.lucene.analysis.ja.JapaneseCompletionAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.density.common.settings.Settings;
import org.density.env.Environment;
import org.density.index.IndexSettings;

public class KuromojiCompletionAnalyzerProvider extends AbstractIndexAnalyzerProvider<JapaneseCompletionAnalyzer> {

    private final JapaneseCompletionAnalyzer analyzer;

    public KuromojiCompletionAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        final JapaneseCompletionFilter.Mode mode = KuromojiCompletionFilterFactory.getMode(settings);
        final UserDictionary userDictionary = KuromojiTokenizerFactory.getUserDictionary(env, settings);
        analyzer = new JapaneseCompletionAnalyzer(userDictionary, mode);
    }

    @Override
    public JapaneseCompletionAnalyzer get() {
        return this.analyzer;
    }

}
