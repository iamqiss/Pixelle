/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.extensions;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.bytes.BytesReference;
import org.density.core.common.io.stream.BytesStreamInput;
import org.density.test.DensityTestCase;

public class ExtensionResponseTests extends DensityTestCase {

    public void testAcknowledgedResponse() throws Exception {
        boolean response = true;
        AcknowledgedResponse booleanResponse = new AcknowledgedResponse(response);

        assertEquals(response, booleanResponse.getStatus());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            booleanResponse.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                booleanResponse = new AcknowledgedResponse(in);

                assertEquals(response, booleanResponse.getStatus());
            }
        }
    }
}
