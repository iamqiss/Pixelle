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

import org.density.Version;
import org.density.cluster.metadata.IndexMetadata;
import org.density.common.settings.Settings;
import org.density.env.Environment;
import org.density.index.IndexSettings;
import org.density.indices.analysis.AnalysisModule;
import org.density.plugins.AnalysisPlugin;
import org.density.test.IndexSettingsModule;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class AnalysisTestsHelper {

    public static DensityTestCase.TestAnalysis createTestAnalysisFromClassPath(
        final Path baseDir,
        final String resource,
        final AnalysisPlugin... plugins
    ) throws IOException {
        final Settings settings = Settings.builder()
            .loadFromStream(resource, AnalysisTestsHelper.class.getResourceAsStream(resource), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), baseDir.toString())
            .build();

        return createTestAnalysisFromSettings(settings, plugins);
    }

    public static DensityTestCase.TestAnalysis createTestAnalysisFromSettings(final Settings settings, final AnalysisPlugin... plugins)
        throws IOException {
        return createTestAnalysisFromSettings(settings, null, plugins);
    }

    public static DensityTestCase.TestAnalysis createTestAnalysisFromSettings(
        final Settings settings,
        final Path configPath,
        final AnalysisPlugin... plugins
    ) throws IOException {
        final Settings actualSettings;
        if (settings.get(IndexMetadata.SETTING_VERSION_CREATED) == null) {
            actualSettings = Settings.builder().put(settings).put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        } else {
            actualSettings = settings;
        }
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", actualSettings);
        final AnalysisRegistry analysisRegistry = new AnalysisModule(new Environment(actualSettings, configPath), Arrays.asList(plugins))
            .getAnalysisRegistry();
        return new DensityTestCase.TestAnalysis(
            analysisRegistry.build(indexSettings),
            analysisRegistry.buildTokenFilterFactories(indexSettings),
            analysisRegistry.buildTokenizerFactories(indexSettings),
            analysisRegistry.buildCharFilterFactories(indexSettings)
        );
    }

}
