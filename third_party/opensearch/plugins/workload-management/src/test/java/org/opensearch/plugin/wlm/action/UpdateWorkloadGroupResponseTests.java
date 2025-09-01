/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.plugin.wlm.action;

import org.density.cluster.metadata.WorkloadGroup;
import org.density.common.io.stream.BytesStreamOutput;
import org.density.common.xcontent.json.JsonXContent;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.rest.RestStatus;
import org.density.core.xcontent.ToXContent;
import org.density.core.xcontent.XContentBuilder;
import org.density.plugin.wlm.WorkloadManagementTestUtils;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.density.plugin.wlm.WorkloadManagementTestUtils.workloadGroupOne;
import static org.mockito.Mockito.mock;

public class UpdateWorkloadGroupResponseTests extends DensityTestCase {

    /**
     * Test case to verify the serialization and deserialization of UpdateWorkloadGroupResponse.
     */
    public void testSerialization() throws IOException {
        UpdateWorkloadGroupResponse response = new UpdateWorkloadGroupResponse(workloadGroupOne, RestStatus.OK);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        UpdateWorkloadGroupResponse otherResponse = new UpdateWorkloadGroupResponse(streamInput);
        assertEquals(response.getRestStatus(), otherResponse.getRestStatus());
        WorkloadGroup responseGroup = response.getWorkloadGroup();
        WorkloadGroup otherResponseGroup = otherResponse.getWorkloadGroup();
        List<WorkloadGroup> list1 = new ArrayList<>();
        List<WorkloadGroup> list2 = new ArrayList<>();
        list1.add(responseGroup);
        list2.add(otherResponseGroup);
        WorkloadManagementTestUtils.assertEqualWorkloadGroups(list1, list2, false);
    }

    /**
     * Test case to verify the toXContent method of UpdateWorkloadGroupResponse.
     */
    public void testToXContentUpdateSingleWorkloadGroup() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint();
        UpdateWorkloadGroupResponse otherResponse = new UpdateWorkloadGroupResponse(workloadGroupOne, RestStatus.OK);
        String actual = otherResponse.toXContent(builder, mock(ToXContent.Params.class)).toString();
        String expected = "{\n"
            + "  \"_id\" : \"AgfUO5Ja9yfsYlONlYi3TQ==\",\n"
            + "  \"name\" : \"workload_group_one\",\n"
            + "  \"resiliency_mode\" : \"monitor\",\n"
            + "  \"resource_limits\" : {\n"
            + "    \"memory\" : 0.3\n"
            + "  },\n"
            + "  \"updated_at\" : 4513232413\n"
            + "}";
        assertEquals(expected, actual);
    }
}
