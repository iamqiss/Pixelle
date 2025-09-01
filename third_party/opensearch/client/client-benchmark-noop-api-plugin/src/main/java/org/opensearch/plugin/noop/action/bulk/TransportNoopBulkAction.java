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

package org.density.plugin.noop.action.bulk;

import org.density.action.DocWriteRequest;
import org.density.action.DocWriteResponse;
import org.density.action.bulk.BulkItemResponse;
import org.density.action.bulk.BulkRequest;
import org.density.action.bulk.BulkResponse;
import org.density.action.support.ActionFilters;
import org.density.action.support.HandledTransportAction;
import org.density.action.update.UpdateResponse;
import org.density.common.inject.Inject;
import org.density.core.action.ActionListener;
import org.density.core.index.shard.ShardId;
import org.density.tasks.Task;
import org.density.transport.TransportService;

public class TransportNoopBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {
    private static final BulkItemResponse ITEM_RESPONSE = new BulkItemResponse(
        1,
        DocWriteRequest.OpType.UPDATE,
        new UpdateResponse(new ShardId("mock", "", 1), "1", 0L, 1L, 1L, DocWriteResponse.Result.CREATED)
    );

    @Inject
    public TransportNoopBulkAction(TransportService transportService, ActionFilters actionFilters) {
        super(NoopBulkAction.NAME, transportService, actionFilters, BulkRequest::new);
    }

    @Override
    protected void doExecute(Task task, BulkRequest request, ActionListener<BulkResponse> listener) {
        final int itemCount = request.requests().size();
        // simulate at least a realistic amount of data that gets serialized
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[itemCount];
        for (int idx = 0; idx < itemCount; idx++) {
            bulkItemResponses[idx] = ITEM_RESPONSE;
        }
        listener.onResponse(new BulkResponse(bulkItemResponses, 0));
    }
}
