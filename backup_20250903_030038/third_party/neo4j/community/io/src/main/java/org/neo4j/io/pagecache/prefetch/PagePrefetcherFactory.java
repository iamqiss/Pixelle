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
package org.neo4j.io.pagecache.prefetch;

import org.neo4j.annotations.service.Service;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.PrioritizedService;
import org.neo4j.service.Services;

@Service
public interface PagePrefetcherFactory extends PrioritizedService {

    PagePrefetcher create(PageCache pageCache, JobScheduler jobScheduler, CursorContextFactory contextFactory);

    static PagePrefetcherFactory getInstance() {
        return PagePrefetcherFactoryHolder.PAGE_PREFETCHER_FACTORY;
    }

    final class PagePrefetcherFactoryHolder {
        private static final PagePrefetcherFactory PAGE_PREFETCHER_FACTORY = loadProvider();

        private static PagePrefetcherFactory loadProvider() {
            return Services.loadByPriority(PagePrefetcherFactory.class).orElseGet(DefaultPagePrefetcherFactory::new);
        }
    }
}
