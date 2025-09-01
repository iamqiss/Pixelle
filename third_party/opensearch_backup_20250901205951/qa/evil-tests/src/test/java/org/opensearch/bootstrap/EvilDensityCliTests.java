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

package org.density.bootstrap;


import org.density.cli.ExitCodes;
import org.density.common.SuppressForbidden;
import org.density.common.io.PathUtils;
import org.density.common.settings.Settings;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class EvilDensityCliTests extends DensityCliTestCase {

    @SuppressForbidden(reason = "manipulates system properties for testing")
    public void testPathHome() throws Exception {
        final String pathHome = System.getProperty("density.path.home");
        final String value = randomAlphaOfLength(16);
        System.setProperty("density.path.home", value);

        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, esSettings) -> {
                    Settings settings = esSettings.settings();
                    assertThat(settings.keySet(), hasSize(2));
                    assertThat(
                        settings.get("path.home"),
                        equalTo(PathUtils.get(System.getProperty("user.dir")).resolve(value).toString()));
                    assertThat(settings.keySet(), hasItem("path.logs")); // added by env initialization
                });

        System.clearProperty("density.path.home");
        final String commandLineValue = randomAlphaOfLength(16);
        runTest(
                ExitCodes.OK,
                true,
                (output, error) -> {},
                (foreground, pidFile, quiet, esSettings) -> {
                    Settings settings = esSettings.settings();
                    assertThat(settings.keySet(), hasSize(2));
                    assertThat(
                        settings.get("path.home"),
                        equalTo(PathUtils.get(System.getProperty("user.dir")).resolve(commandLineValue).toString()));
                    assertThat(settings.keySet(), hasItem("path.logs")); // added by env initialization
                },
                "-Epath.home=" + commandLineValue);

        if (pathHome != null) System.setProperty("density.path.home", pathHome);
        else System.clearProperty("density.path.home");
    }

}
