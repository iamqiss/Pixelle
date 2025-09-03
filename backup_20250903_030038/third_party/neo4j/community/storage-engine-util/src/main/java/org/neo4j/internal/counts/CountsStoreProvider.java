/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.counts;

import java.nio.file.OpenOption;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsStore;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.impl.muninn.VersionStorage;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.service.PrioritizedService;
import org.neo4j.service.Services;

@Service
public interface CountsStoreProvider extends PrioritizedService {

    CountsStore openCountsStore(
            PageCache pageCache,
            FileSystemAbstraction fs,
            RecordDatabaseLayout layout,
            InternalLogProvider userLogProvider,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            Config config,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            ImmutableSet<OpenOption> openOptions,
            CountsBuilder initialCountsBuilder,
            boolean readOnly,
            VersionStorage versionStorage);

    static CountsStoreProvider getInstance() {
        return CountsStoreProviderHolder.COUNTS_STORE_PROVIDER;
    }

    final class CountsStoreProviderHolder {
        private static final CountsStoreProvider COUNTS_STORE_PROVIDER = loadProvider();

        private static CountsStoreProvider loadProvider() {
            return Services.loadByPriority(CountsStoreProvider.class)
                    .orElseThrow(
                            () -> new IllegalStateException("Failed to load instance of " + CountsStoreProvider.class));
        }
    }
}
