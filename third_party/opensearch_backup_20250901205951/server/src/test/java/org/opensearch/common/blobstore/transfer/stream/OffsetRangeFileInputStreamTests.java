/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.blobstore.transfer.stream;

import java.io.IOException;

public class OffsetRangeFileInputStreamTests extends ResettableCheckedInputStreamBaseTest {

    @Override
    protected OffsetRangeInputStream getOffsetRangeInputStream(long size, long position) throws IOException {
        return new OffsetRangeFileInputStream(testFile, size, position);
    }
}
