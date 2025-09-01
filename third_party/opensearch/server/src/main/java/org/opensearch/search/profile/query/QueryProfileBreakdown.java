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

package org.density.search.profile.query;

import org.density.search.profile.AbstractProfileBreakdown;
import org.density.search.profile.ContextualProfileBreakdown;
import org.density.search.profile.ProfileMetric;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * A record of timings for the various operations that may happen during query execution.
 * A node's time may be composed of several internal attributes (rewriting, weighting,
 * scoring, etc).
 *
 * @density.internal
 */
public final class QueryProfileBreakdown extends ContextualProfileBreakdown {

    public QueryProfileBreakdown(Collection<Supplier<ProfileMetric>> metrics) {
        super(metrics);
    }

    @Override
    public AbstractProfileBreakdown context(Object context) {
        return this;
    }

}
