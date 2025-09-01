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
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.density.plugin.wlm.WorkloadManagementTestUtils.assertEqualWorkloadGroups;
import static org.density.plugin.wlm.WorkloadManagementTestUtils.workloadGroupOne;

public class CreateWorkloadGroupRequestTests extends DensityTestCase {

    /**
     * Test case to verify the serialization and deserialization of CreateWorkloadGroupRequest.
     */
    public void testSerialization() throws IOException {
        CreateWorkloadGroupRequest request = new CreateWorkloadGroupRequest(workloadGroupOne);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        CreateWorkloadGroupRequest otherRequest = new CreateWorkloadGroupRequest(streamInput);
        List<WorkloadGroup> list1 = new ArrayList<>();
        List<WorkloadGroup> list2 = new ArrayList<>();
        list1.add(workloadGroupOne);
        list2.add(otherRequest.getWorkloadGroup());
        assertEqualWorkloadGroups(list1, list2, false);
    }
}
