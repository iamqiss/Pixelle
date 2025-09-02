/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter.Mode;
import org.density.common.settings.Settings;
import org.density.env.Environment;
import org.density.index.IndexSettings;

public class KuromojiCompletionFilterFactory extends AbstractTokenFilterFactory {
    private final Mode mode;

    public KuromojiCompletionFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.mode = getMode(settings);
    }

    public static Mode getMode(Settings settings) {
        String modeSetting = settings.get("mode", null);
        if (modeSetting != null) {
            if ("index".equalsIgnoreCase(modeSetting)) {
                return Mode.INDEX;
            } else if ("query".equalsIgnoreCase(modeSetting)) {
                return Mode.QUERY;
            }
        }
        return Mode.INDEX;
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new JapaneseCompletionFilter(tokenStream, mode);
    }
}
