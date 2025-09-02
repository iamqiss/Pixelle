/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.indices.recovery;

import org.density.core.action.ActionListener;
import org.density.core.common.bytes.BytesReference;
import org.density.index.store.StoreFileMetadata;

/**
 * Writes a partial file chunk to the target store.
 *
 * @density.internal
 */
@FunctionalInterface
public interface FileChunkWriter {

    void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    );

    default void cancel() {}
}
