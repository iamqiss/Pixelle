/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.rule.action;

import org.density.common.io.stream.BytesStreamOutput;
import org.density.core.common.io.stream.StreamInput;
import org.density.core.common.io.stream.Writeable;
import org.density.test.DensityTestCase;

import java.io.IOException;

public class DeleteRuleActionTests extends DensityTestCase {
    public void testGetName() {
        assertEquals("cluster:admin/density/rule/_delete", DeleteRuleAction.NAME);
    }

    public void testGetResponseReader() throws IOException {
        assertTrue(DeleteRuleAction.INSTANCE.getResponseReader() instanceof Writeable.Reader);

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeBoolean(true);
        StreamInput in = out.bytes().streamInput();

        assertNotNull(DeleteRuleAction.INSTANCE.getResponseReader().read(in));
    }
}
