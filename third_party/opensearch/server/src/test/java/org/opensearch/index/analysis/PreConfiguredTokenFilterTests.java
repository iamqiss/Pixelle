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

package org.density.index.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.env.Environment;
import org.density.env.TestEnvironment;
import org.density.index.IndexSettings;
import org.density.test.IndexSettingsModule;
import org.density.test.DensityTestCase;
import org.density.test.VersionUtils;

import java.io.IOException;

public class PreConfiguredTokenFilterTests extends DensityTestCase {

    private final Settings emptyNodeSettings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    public void testCachingWithSingleton() throws IOException {
        PreConfiguredTokenFilter pctf = PreConfiguredTokenFilter.singleton(
            "singleton",
            randomBoolean(),
            (tokenStream) -> new TokenFilter(tokenStream) {
                @Override
                public boolean incrementToken() {
                    return false;
                }
            }
        );

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        Version version1 = VersionUtils.randomVersion(random());
        Settings settings1 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version1).build();
        TokenFilterFactory tff_v1_1 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings1);
        TokenFilterFactory tff_v1_2 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings1);
        assertSame(tff_v1_1, tff_v1_2);

        Version version2 = randomValueOtherThan(version1, () -> randomFrom(VersionUtils.allVersions()));
        Settings settings2 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version2).build();

        TokenFilterFactory tff_v2 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "singleton", settings2);
        assertSame(tff_v1_1, tff_v2);
    }

    public void testCachingWithDensityVersion() throws IOException {
        PreConfiguredTokenFilter pctf = PreConfiguredTokenFilter.openSearchVersion(
            "density_version",
            randomBoolean(),
            (tokenStream, esVersion) -> new TokenFilter(tokenStream) {
                @Override
                public boolean incrementToken() {
                    return false;
                }
            }
        );

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        Version version1 = VersionUtils.randomVersion(random());
        Settings settings1 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version1).build();
        TokenFilterFactory tff_v1_1 = pctf.get(
            indexSettings,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "density_version",
            settings1
        );
        TokenFilterFactory tff_v1_2 = pctf.get(
            indexSettings,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "density_version",
            settings1
        );
        assertSame(tff_v1_1, tff_v1_2);

        Version version2 = randomValueOtherThan(version1, () -> randomFrom(VersionUtils.allVersions()));
        Settings settings2 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version2).build();

        TokenFilterFactory tff_v2 = pctf.get(
            indexSettings,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "density_version",
            settings2
        );
        assertNotSame(tff_v1_1, tff_v2);
    }

    public void testCachingWithLuceneVersion() throws IOException {
        PreConfiguredTokenFilter pctf = PreConfiguredTokenFilter.luceneVersion(
            "lucene_version",
            randomBoolean(),
            (tokenStream, luceneVersion) -> new TokenFilter(tokenStream) {
                @Override
                public boolean incrementToken() {
                    return false;
                }
            }
        );

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);

        Version version1 = Version.CURRENT;
        Settings settings1 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version1).build();
        TokenFilterFactory tff_v1_1 = pctf.get(
            indexSettings,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "lucene_version",
            settings1
        );
        TokenFilterFactory tff_v1_2 = pctf.get(
            indexSettings,
            TestEnvironment.newEnvironment(emptyNodeSettings),
            "lucene_version",
            settings1
        );
        assertSame(tff_v1_1, tff_v1_2);

        byte major = VersionUtils.getPreviousVersion().major;
        Version version2 = Version.fromString(major - 1 + ".0.0");
        Settings settings2 = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version2).build();

        TokenFilterFactory tff_v2 = pctf.get(indexSettings, TestEnvironment.newEnvironment(emptyNodeSettings), "lucene_version", settings2);
        assertNotSame(tff_v1_1, tff_v2);
    }
}
