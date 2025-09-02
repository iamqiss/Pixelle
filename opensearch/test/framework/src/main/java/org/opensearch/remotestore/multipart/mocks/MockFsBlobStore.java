/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.density.remotestore.multipart.mocks;

import org.density.DensityException;
import org.density.common.blobstore.BlobContainer;
import org.density.common.blobstore.BlobPath;
import org.density.common.blobstore.fs.FsBlobStore;

import java.io.IOException;
import java.nio.file.Path;

public class MockFsBlobStore extends FsBlobStore {

    private final boolean triggerDataIntegrityFailure;

    public MockFsBlobStore(int bufferSizeInBytes, Path path, boolean readonly, boolean triggerDataIntegrityFailure) throws IOException {
        super(bufferSizeInBytes, path, readonly);
        this.triggerDataIntegrityFailure = triggerDataIntegrityFailure;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        try {
            return new MockFsAsyncBlobContainer(this, path, buildAndCreate(path), triggerDataIntegrityFailure);
        } catch (IOException ex) {
            throw new DensityException("failed to create blob container", ex);
        }
    }
}
