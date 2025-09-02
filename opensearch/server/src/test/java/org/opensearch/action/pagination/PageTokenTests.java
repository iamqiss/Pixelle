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

public class PageTokenTests extends DensityTestCase {

    public void testSerialization() throws Exception {
        PageToken pageToken = new PageToken("foo", "test");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            pageToken.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                PageToken deserialized = new PageToken(in);
                assertEquals(pageToken, deserialized);
            }
        }
    }

    public void testSerializationWithNextTokenAbsent() throws Exception {
        PageToken pageToken = new PageToken(null, "test");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            pageToken.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                PageToken deserialized = new PageToken(in);
                assertEquals(pageToken, deserialized);
            }
        }
    }
}
