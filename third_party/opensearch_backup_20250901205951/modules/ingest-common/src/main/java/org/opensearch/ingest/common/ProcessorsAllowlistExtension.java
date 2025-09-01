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

package org.density.ingest.common;

import org.density.painless.spi.Allowlist;
import org.density.painless.spi.AllowlistLoader;
import org.density.painless.spi.PainlessExtension;
import org.density.script.IngestScript;
import org.density.script.ScriptContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ProcessorsAllowlistExtension implements PainlessExtension {

    private static final Allowlist ALLOWLIST = AllowlistLoader.loadFromResourceFiles(
        ProcessorsAllowlistExtension.class,
        "processors_allowlist.txt"
    );

    @Override
    public Map<ScriptContext<?>, List<Allowlist>> getContextAllowlists() {
        return Collections.singletonMap(IngestScript.CONTEXT, Collections.singletonList(ALLOWLIST));
    }
}
