/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.index.store;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.density.common.blobstore.BlobContainer;

/**
 * A {@code RemoteBufferedOutputDirectory} is an extension of RemoteDirectory which also provides an abstraction layer
 * for storing a list of files to a remote store.
 * Additionally, with this implementation, creation of new files is also allowed.
 * A remoteDirectory contains only files (no sub-folder hierarchy).
 *
 * @density.internal
 */
public class RemoteBufferedOutputDirectory extends RemoteDirectory {
    public RemoteBufferedOutputDirectory(BlobContainer blobContainer) {
        super(blobContainer);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) {
        return new RemoteBufferedIndexOutput(name, this.blobContainer);
    }
}
