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

package org.density.plugin.noop.action.search;

import org.apache.lucene.search.TotalHits;
import org.density.action.search.SearchRequest;
import org.density.action.search.SearchResponse;
import org.density.action.search.ShardSearchFailure;
import org.density.action.support.ActionFilters;
import org.density.action.support.HandledTransportAction;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.common.io.stream.Writeable;
import org.density.search.SearchHit;
import org.density.search.SearchHits;
import org.density.search.aggregations.InternalAggregations;
import org.density.search.internal.InternalSearchResponse;
import org.density.search.profile.SearchProfileShardResults;
import org.density.search.suggest.Suggest;
import org.density.tasks.Task;
import org.density.transport.TransportService;

import java.util.Collections;

public class TransportNoopSearchAction extends HandledTransportAction<SearchRequest, SearchResponse> {
    @Inject
    public TransportNoopSearchAction(TransportService transportService, ActionFilters actionFilters) {
        super(NoopSearchAction.NAME, transportService, actionFilters, (Writeable.Reader<SearchRequest>) SearchRequest::new);
    }

    @Override
    protected void doExecute(Task task, SearchRequest request, ActionListener<SearchResponse> listener) {
        listener.onResponse(
            new SearchResponse(
                new InternalSearchResponse(
                    new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0f),
                    InternalAggregations.EMPTY,
                    new Suggest(Collections.emptyList()),
                    new SearchProfileShardResults(Collections.emptyMap()),
                    false,
                    false,
                    1
                ),
                "",
                1,
                1,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            )
        );
    }
}
