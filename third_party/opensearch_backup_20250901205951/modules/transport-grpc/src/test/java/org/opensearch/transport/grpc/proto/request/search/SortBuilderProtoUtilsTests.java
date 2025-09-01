/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.transport.grpc.proto.request.search;

import org.density.search.sort.SortBuilder;
import org.density.test.DensityTestCase;
import org.density.transport.grpc.proto.request.search.sort.SortBuilderProtoUtils;

import java.util.Collections;
import java.util.List;

public class SortBuilderProtoUtilsTests extends DensityTestCase {

    public void testFromProtoWithEmptyList() {
        // Call the method under test with an empty list
        List<SortBuilder<?>> sortBuilders = SortBuilderProtoUtils.fromProto(Collections.emptyList());

        // Verify the result
        assertNotNull("SortBuilders list should not be null", sortBuilders);
        assertTrue("SortBuilders list should be empty", sortBuilders.isEmpty());
    }
}
