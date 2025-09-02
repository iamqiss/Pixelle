/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The Density Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright Density Contributors. See
 * GitHub history for details.
 */

package org.density.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.SearcherManager;
import org.density.common.SuppressForbidden;
import org.density.common.lucene.index.DensityDirectoryReader;

import java.io.IOException;

/**
 * Utility class to safely share {@link DensityDirectoryReader} instances across
 * multiple threads, while periodically reopening. This class ensures each
 * reader is closed only once all threads have finished using it.
 *
 * @see SearcherManager
 *
 * @density.internal
 */
@SuppressForbidden(reason = "reference counting is required here")
class DensityReaderManager extends ReferenceManager<DensityDirectoryReader> {
    /**
     * Creates and returns a new DensityReaderManager from the given
     * already-opened {@link DensityDirectoryReader}, stealing
     * the incoming reference.
     *
     * @param reader            the directoryReader to use for future reopens
     */
    DensityReaderManager(DensityDirectoryReader reader) {
        this.current = reader;
    }

    @Override
    protected void decRef(DensityDirectoryReader reference) throws IOException {
        reference.decRef();
    }

    @Override
    protected DensityDirectoryReader refreshIfNeeded(DensityDirectoryReader referenceToRefresh) throws IOException {
        final DensityDirectoryReader reader = (DensityDirectoryReader) DirectoryReader.openIfChanged(referenceToRefresh);
        return reader;
    }

    @Override
    protected boolean tryIncRef(DensityDirectoryReader reference) {
        return reference.tryIncRef();
    }

    @Override
    protected int getRefCount(DensityDirectoryReader reference) {
        return reference.getRefCount();
    }
}
