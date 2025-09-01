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

package org.density.rest.action.cat;

import org.density.action.ActionRequest;
import org.density.action.ActionType;
import org.density.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.density.cluster.node.DiscoveryNodes;
import org.density.common.collect.MapBuilder;
import org.density.core.action.ActionListener;
import org.density.core.action.ActionResponse;
import org.density.core.tasks.TaskId;
import org.density.core.xcontent.NamedXContentRegistry;
import org.density.tasks.TaskInfo;
import org.density.test.DensityTestCase;
import org.density.test.client.NoOpNodeClient;
import org.density.test.rest.FakeRestChannel;
import org.density.test.rest.FakeRestRequest;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.density.tasks.TaskInfoTests.randomResourceStats;
import static org.hamcrest.Matchers.is;

public class RestTasksActionTests extends DensityTestCase {

    public void testConsumesParameters() throws Exception {
        RestTasksAction action = new RestTasksAction(() -> DiscoveryNodes.EMPTY_NODES);
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            MapBuilder.<String, String>newMapBuilder()
                .put("parent_task_id", "the node:3")
                .put("nodes", "node1,node2")
                .put("actions", "*")
                .map()
        ).build();
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, false, 1);
        try (NoOpNodeClient nodeClient = buildNodeClient()) {
            action.handleRequest(fakeRestRequest, fakeRestChannel, nodeClient);
        }

        assertThat(fakeRestChannel.errors().get(), is(0));
        assertThat(fakeRestChannel.responses().get(), is(1));
    }

    private NoOpNodeClient buildNodeClient() {
        return new NoOpNodeClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                final TaskInfo taskInfo = new TaskInfo(
                    new TaskId("test-node-id", randomLong()),
                    "test_type",
                    "test_action",
                    "test_description",
                    null,
                    randomLong(),
                    randomLongBetween(0, Long.MAX_VALUE),
                    false,
                    false,
                    TaskId.EMPTY_TASK_ID,
                    Map.of("foo", "bar"),
                    randomResourceStats(randomBoolean())
                );
                listener.onResponse((Response) new ListTasksResponse(List.of(taskInfo), emptyList(), emptyList()));
            }
        };
    }
}
