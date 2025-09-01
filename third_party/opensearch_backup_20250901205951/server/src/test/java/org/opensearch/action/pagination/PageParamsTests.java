/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.pagination;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.test.DensityTestCase;

public class PageParamsTests extends DensityTestCase {

    public void testSerialization() throws Exception {
        PageParams pageParams = new PageParams("foo", "foo", 1);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            pageParams.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(pageParams, new PageParams(in));
            }
        }
    }

    public void testSerializationWithRequestedTokenAndSortAbsent() throws Exception {
        PageParams pageParams = new PageParams(null, null, 1);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            pageParams.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(pageParams, new PageParams(in));
            }
        }
    }

}
