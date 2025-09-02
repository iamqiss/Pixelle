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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.density.common.lucene.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.density.common.SuppressForbidden;
import org.density.common.annotation.PublicApi;
import org.density.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

/**
 * A {@link org.apache.lucene.index.FilterDirectoryReader} that exposes
 * Density internal per shard / index information like the shard ID.
 *
 * @density.api
 */
@PublicApi(since = "1.0.0")
public final class DensityDirectoryReader extends FilterDirectoryReader {

    private final ShardId shardId;
    private final FilterDirectoryReader.SubReaderWrapper wrapper;

    private final DelegatingCacheHelper delegatingCacheHelper;

    private DensityDirectoryReader(DirectoryReader in, FilterDirectoryReader.SubReaderWrapper wrapper, ShardId shardId)
        throws IOException {
        super(in, wrapper);
        this.wrapper = wrapper;
        this.shardId = shardId;
        this.delegatingCacheHelper = new DelegatingCacheHelper(in.getReaderCacheHelper());
    }

    /**
     * Returns the shard id this index belongs to.
     */
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        // safe to delegate since this reader does not alter the index
        return this.delegatingCacheHelper;
    }

    public DelegatingCacheHelper getDelegatingCacheHelper() {
        return this.delegatingCacheHelper;
    }

    /**
     * Wraps existing IndexReader cache helper which internally provides a way to wrap CacheKey.
     *
     * @density.api
     */
    @PublicApi(since = "2.13.0")
    public class DelegatingCacheHelper implements CacheHelper {
        private final CacheHelper cacheHelper;
        private final DelegatingCacheKey serializableCacheKey;

        DelegatingCacheHelper(CacheHelper cacheHelper) {
            this.cacheHelper = cacheHelper;
            this.serializableCacheKey = new DelegatingCacheKey(Optional.ofNullable(cacheHelper).map(key -> getKey()).orElse(null));
        }

        @Override
        public CacheKey getKey() {
            return this.cacheHelper.getKey();
        }

        public DelegatingCacheKey getDelegatingCacheKey() {
            return this.serializableCacheKey;
        }

        @Override
        public void addClosedListener(ClosedListener listener) {
            this.cacheHelper.addClosedListener(listener);
        }
    }

    /**
     *  Wraps internal IndexReader.CacheKey and attaches a uniqueId to it which can be eventually be used instead of
     *  object itself for serialization purposes.
     *
     *  @density.api
     */
    @PublicApi(since = "2.13.0")
    public class DelegatingCacheKey {
        private final CacheKey cacheKey;
        private final String uniqueId;

        DelegatingCacheKey(CacheKey cacheKey) {
            this.cacheKey = cacheKey;
            this.uniqueId = UUID.randomUUID().toString();
        }

        public CacheKey getCacheKey() {
            return this.cacheKey;
        }

        public String getId() {
            return uniqueId;
        }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
        return new DensityDirectoryReader(in, wrapper, shardId);
    }

    /**
     * Wraps the given reader in a {@link DensityDirectoryReader} as
     * well as all it's sub-readers in {@link DensityLeafReader} to
     * expose the given shard Id.
     *
     * @param reader the reader to wrap
     * @param shardId the shard ID to expose via the density internal reader wrappers.
     */
    public static DensityDirectoryReader wrap(DirectoryReader reader, ShardId shardId) throws IOException {
        return new DensityDirectoryReader(reader, new SubReaderWrapper(shardId), shardId);
    }

    private static final class SubReaderWrapper extends FilterDirectoryReader.SubReaderWrapper {
        private final ShardId shardId;

        SubReaderWrapper(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public LeafReader wrap(LeafReader reader) {
            return new DensityLeafReader(reader, shardId);
        }
    }

    /**
     * Adds the given listener to the provided directory reader. The reader
     * must contain an {@link DensityDirectoryReader} in it's hierarchy
     * otherwise we can't safely install the listener.
     *
     * @throws IllegalArgumentException if the reader doesn't contain an
     *     {@link DensityDirectoryReader} in it's hierarchy
     */
    @SuppressForbidden(reason = "This is the only sane way to add a ReaderClosedListener")
    public static void addReaderCloseListener(DirectoryReader reader, IndexReader.ClosedListener listener) {
        DensityDirectoryReader openSearchDirectoryReader = getDensityDirectoryReader(reader);
        if (openSearchDirectoryReader == null) {
            throw new IllegalArgumentException(
                "Can't install close listener reader is not an DensityDirectoryReader/DensityLeafReader"
            );
        }
        IndexReader.CacheHelper cacheHelper = openSearchDirectoryReader.getReaderCacheHelper();
        if (cacheHelper == null) {
            throw new IllegalArgumentException("Reader " + openSearchDirectoryReader + " does not support caching");
        }
        assert cacheHelper.getKey() == reader.getReaderCacheHelper().getKey();
        cacheHelper.addClosedListener(listener);
    }

    /**
     * Tries to unwrap the given reader until the first
     * {@link DensityDirectoryReader} instance is found or {@code null}
     * if no instance is found.
     */
    public static DensityDirectoryReader getDensityDirectoryReader(DirectoryReader reader) {
        if (reader instanceof FilterDirectoryReader) {
            if (reader instanceof DensityDirectoryReader) {
                return (DensityDirectoryReader) reader;
            } else {
                // We need to use FilterDirectoryReader#getDelegate and not FilterDirectoryReader#unwrap, because
                // If there are multiple levels of filtered leaf readers then with the unwrap() method it immediately
                // returns the most inner leaf reader and thus skipping of over any other filtered leaf reader that
                // may be instance of DensityLeafReader. This can cause us to miss the shardId.
                return getDensityDirectoryReader(((FilterDirectoryReader) reader).getDelegate());
            }
        }
        return null;
    }
}
