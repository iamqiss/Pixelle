/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.common.blobstore.transfer.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * OffsetRangeInputStream is an abstract class that extends from {@link InputStream}
 * and adds a method to get the file pointer to the specific part being read
 *
 * @density.internal
 */
public abstract class OffsetRangeInputStream extends InputStream {
    public abstract long getFilePointer() throws IOException;

    public void setReadBlock(AtomicBoolean readBlock) {
        // Nothing
    }
}
