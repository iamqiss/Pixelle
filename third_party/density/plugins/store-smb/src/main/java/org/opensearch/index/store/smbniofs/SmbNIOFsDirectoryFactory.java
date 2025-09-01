/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store.smbniofs;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NIOFSDirectory;
import org.density.index.IndexSettings;
import org.density.index.store.FsDirectoryFactory;
import org.density.index.store.SmbDirectoryWrapper;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Factory to create a new NIO File System type directory accessible as a SMB share
 */
public final class SmbNIOFsDirectoryFactory extends FsDirectoryFactory {

    @Override
    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        return new SmbDirectoryWrapper(new NIOFSDirectory(location, lockFactory));
    }
}
