/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.translog.transfer;

import org.density.index.translog.transfer.FileSnapshot.TransferFileSnapshot;

/**
 * Exception when a single file transfer encounters a failure
 *
 * @density.internal
 */
public class FileTransferException extends RuntimeException {

    private final TransferFileSnapshot fileSnapshot;

    public FileTransferException(TransferFileSnapshot fileSnapshot, Throwable cause) {
        super(cause);
        this.fileSnapshot = fileSnapshot;
    }

    public TransferFileSnapshot getFileSnapshot() {
        return fileSnapshot;
    }
}
