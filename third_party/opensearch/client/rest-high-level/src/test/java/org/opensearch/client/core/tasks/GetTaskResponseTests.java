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

package org.density.client.core.tasks;

import org.density.client.tasks.GetTaskResponse;
import org.density.core.common.bytes.BytesReference;
import org.density.core.tasks.TaskId;
import org.density.core.tasks.resourcetracker.TaskResourceStats;
import org.density.core.tasks.resourcetracker.TaskResourceUsage;
import org.density.core.tasks.resourcetracker.TaskThreadUsage;
import org.density.core.xcontent.MediaTypeRegistry;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.tasks.RawTaskStatus;
import org.density.tasks.Task;
import org.density.tasks.TaskInfo;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.density.test.AbstractXContentTestCase.xContentTester;

public class GetTaskResponseTests extends DensityTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser, this::createTestInstance, this::toXContent, GetTaskResponse::fromXContent).supportsUnknownFields(
            true
        )
            .assertEqualsConsumer(this::assertEqualInstances)
            .assertToXContentEquivalence(true)
            .randomFieldsExcludeFilter(field -> field.endsWith("headers") || field.endsWith("status") || field.contains("resource_stats"))
            .test();
    }

    private GetTaskResponse createTestInstance() {
        return new GetTaskResponse(randomBoolean(), randomTaskInfo());
    }

    private void toXContent(GetTaskResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.field(GetTaskResponse.COMPLETED.getPreferredName(), response.isCompleted());
            builder.startObject(GetTaskResponse.TASK.getPreferredName());
            response.getTaskInfo().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
        builder.endObject();
    }

    private void assertEqualInstances(GetTaskResponse expectedInstance, GetTaskResponse newInstance) {
        assertEquals(expectedInstance.isCompleted(), newInstance.isCompleted());
        assertEquals(expectedInstance.getTaskInfo(), newInstance.getTaskInfo());
    }

    static TaskInfo randomTaskInfo() {
        TaskId taskId = randomTaskId();
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(5);
        Task.Status status = randomBoolean() ? randomRawTaskStatus() : null;
        String description = randomBoolean() ? randomAlphaOfLength(5) : null;
        long startTime = randomLong();
        long runningTimeNanos = randomLong();
        boolean cancellable = randomBoolean();
        boolean cancelled = cancellable == true ? randomBoolean() : false;
        TaskId parentTaskId = randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId();
        Long cancellationStartTime = null;
        if (cancelled) {
            cancellationStartTime = randomNonNegativeLong();
        }
        Map<String, String> headers = randomBoolean()
            ? Collections.emptyMap()
            : Collections.singletonMap(randomAlphaOfLength(5), randomAlphaOfLength(5));
        return new TaskInfo(
            taskId,
            type,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers,
            randomResourceStats(),
            cancellationStartTime
        );
    }

    private static TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLength(5), randomLong());
    }

    private static RawTaskStatus randomRawTaskStatus() {
        try (XContentBuilder builder = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent())) {
            builder.startObject();
            int fields = between(0, 10);
            for (int f = 0; f < fields; f++) {
                builder.field(randomAlphaOfLength(5), randomAlphaOfLength(5));
            }
            builder.endObject();
            return new RawTaskStatus(BytesReference.bytes(builder));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static TaskResourceStats randomResourceStats() {
        return randomBoolean() ? null : new TaskResourceStats(new HashMap<>() {
            {
                for (int i = 0; i < randomInt(5); i++) {
                    put(randomAlphaOfLength(5), new TaskResourceUsage(randomNonNegativeLong(), randomNonNegativeLong()));
                }
            }
        }, new TaskThreadUsage(randomInt(10), randomInt(10)));
    }
}
