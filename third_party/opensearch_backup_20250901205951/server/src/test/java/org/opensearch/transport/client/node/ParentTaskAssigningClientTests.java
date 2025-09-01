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

package org.density.transport.client.node;

import org.density.action.ActionRequest;
import org.density.action.ActionType;
import org.density.action.bulk.BulkRequest;
import org.density.action.search.ClearScrollRequest;
import org.density.action.search.SearchRequest;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.tasks.TaskId;
import org.density.test.DensityTestCase;
import org.density.test.client.NoOpClient;
import org.density.transport.client.ParentTaskAssigningClient;

public class ParentTaskAssigningClientTests extends DensityTestCase {
    public void testSetsParentId() {
        TaskId[] parentTaskId = new TaskId[] { new TaskId(randomAlphaOfLength(3), randomLong()) };

        // This mock will do nothing but verify that parentTaskId is set on all requests sent to it.
        NoOpClient mock = new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertEquals(parentTaskId[0], request.getParentTask());
                super.doExecute(action, request, listener);
            }
        };
        try (ParentTaskAssigningClient client = new ParentTaskAssigningClient(mock, parentTaskId[0])) {
            // All of these should have the parentTaskId set
            client.bulk(new BulkRequest());
            client.search(new SearchRequest());
            client.clearScroll(new ClearScrollRequest());

            // Now lets verify that unwrapped calls don't have the parentTaskId set
            parentTaskId[0] = TaskId.EMPTY_TASK_ID;
            client.unwrap().bulk(new BulkRequest());
            client.unwrap().search(new SearchRequest());
            client.unwrap().clearScroll(new ClearScrollRequest());
        }
    }
}
