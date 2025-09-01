/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.action.admin.indices.view;

import org.density.core.common.io.stream.Writeable;
import org.density.test.AbstractWireSerializingTestCase;

public class ListViewNamesResponseTests extends AbstractWireSerializingTestCase<ListViewNamesAction.Response> {

    @Override
    protected Writeable.Reader<ListViewNamesAction.Response> instanceReader() {
        return ListViewNamesAction.Response::new;
    }

    @Override
    protected ListViewNamesAction.Response createTestInstance() {
        return new ListViewNamesAction.Response(randomList(5, () -> randomAlphaOfLength(8)));
    }
}
