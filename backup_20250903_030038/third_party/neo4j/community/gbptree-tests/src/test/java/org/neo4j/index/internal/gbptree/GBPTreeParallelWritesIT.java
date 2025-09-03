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
package org.neo4j.index.internal.gbptree;

import static java.lang.Integer.min;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.GBPTreeTestUtil.consistencyCheckStrict;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.Race.throwing;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
abstract class GBPTreeParallelWritesIT<KEY, VALUE> {

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private RandomSupport random;

    private DefaultPageCacheTracer pageCacheTracer;
    private PageCache pageCache;
    protected TestLayout<KEY, VALUE> layout;
    private ImmutableSet<OpenOption> openOptions;

    @BeforeEach
    void start() {
        pageCacheTracer = new DefaultPageCacheTracer();
        pageCache = PageCacheSupportExtension.getPageCache(
                fileSystem, config().withPageSize(256).withAccessChecks(true).withTracer(pageCacheTracer));

        openOptions = getOpenOptions();
        layout = getLayout(random, GBPTreeTestUtil.calculatePayloadSize(pageCache, openOptions));
    }

    @AfterEach
    void stop() {
        pageCache.close();
    }

    abstract TestLayout<KEY, VALUE> getLayout(RandomSupport random, int payloadSize);

    ImmutableSet<OpenOption> getOpenOptions() {
        return Sets.immutable.empty();
    }

    @Test
    void shouldDoRandomWritesInParallel() throws IOException {
        // given
        try (var index = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("index"), layout)
                .with(pageCacheTracer)
                .with(openOptions)
                .build()) {
            var threads = Runtime.getRuntime().availableProcessors();
            TreeMap<Long, Pair<KEY, VALUE>>[] dataPerThread = new TreeMap[threads];
            for (int i = 0; i < threads; i++) {
                dataPerThread[i] = new TreeMap<>();
            }

            // when
            var seed = random.seed();
            for (int round = 0; round < 5; round++) {
                var race = new Race();
                var cursorContext = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER).create("test");
                for (var i = 0; i < threads; i++) {
                    int id = i;
                    var threadSeed = seed++;
                    race.addContestant(
                            throwing(() -> {
                                var random = new Random(threadSeed);
                                var data = dataPerThread[id];
                                try (var writer = index.writer(cursorContext)) {
                                    for (int j = 0; j < 2_000; j++) {
                                        var v = random.nextFloat();
                                        var entrySeed = random.nextLong(1_000) * threads + id;
                                        var key = layout.key(entrySeed);
                                        if (v < 0.8) {
                                            // insert/overwrite
                                            var value = layout.value(entrySeed);
                                            writer.put(key, value);
                                            data.put(entrySeed, Pair.of(key, value));
                                        } else {
                                            // remove
                                            writer.remove(key);
                                            data.remove(entrySeed);
                                        }
                                    }
                                }
                            }),
                            1);
                }
                race.goUnchecked();
                index.checkpoint(FileFlushEvent.NULL, cursorContext);
            }

            // then
            MutableLongObjectMap<Pair<KEY, VALUE>> combined = LongObjectMaps.mutable.empty();
            for (var data : dataPerThread) {
                data.forEach(
                        (key, value) -> assertThat(combined.put(key, value)).isNull());
            }
            try (var seek = allEntriesSeek(index, layout)) {
                while (seek.next()) {
                    var removed = combined.remove(layout.keySeed(seek.key()));
                    assertThat(removed).isNotNull();
                    assertThat(layout.compare(removed.getLeft(), seek.key())).isZero();
                    assertThat(layout.compareValue(removed.getRight(), seek.value()))
                            .isZero();
                }
                assertThat(combined).isEmpty();
            }
        }
    }

    @Test
    void shouldWriteInParallelThroughCheckpoints() throws Exception {
        // given
        var openOptions = getOpenOptions();
        var layout = getLayout(random, GBPTreeTestUtil.calculatePayloadSize(pageCache, openOptions));
        try (var tree = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("index"), layout)
                .with(openOptions)
                .build()) {
            var numTasks = 10_000;
            var numCompletedTasks = new LongAdder();
            var numThreads = 8;
            var nextId = new AtomicLong();
            var committedIds = new Ids();
            var race = new Race().withEndCondition(() -> numCompletedTasks.longValue() > numTasks);
            var maxCheckpointDelay = random.among(new int[] {10, 20, 50, 100, 200, 500, 1000, 2000});
            race.addContestants(numThreads, throwing(() -> {
                var rng = ThreadLocalRandom.current();
                var create = numCompletedTasks.floatValue() / numTasks < rng.nextFloat();
                var numEntries = random.nextInt(1, 5);
                if (create) {
                    var ids = new long[numEntries];
                    synchronized (committedIds) {
                        for (var i = 0; i < numEntries; i++) {
                            var change = !committedIds.created.isEmpty() && rng.nextInt(10) == 0;
                            if (change) {
                                // Remove it from created list temporarily so that no other concurrent thread will also
                                // delete/change this ID
                                // since it will be hard to make test assertions otherwise
                                ids[i] = committedIds.created.removeAtIndex(rng.nextInt(committedIds.created.size()));
                            } else {
                                ids[i] = !committedIds.deleted.isEmpty()
                                        ? committedIds.deleted.removeAtIndex(
                                                random.nextInt(committedIds.deleted.size()))
                                        : nextId.getAndIncrement();
                            }
                        }
                    }
                    try (var writer = tree.writer(NULL_CONTEXT)) {
                        for (var id : ids) {
                            writer.put(layout.key(id), layout.value(id));
                        }
                    }
                    synchronized (committedIds) {
                        committedIds.created.addAll(ids);
                    }
                } else {
                    long[] ids;
                    synchronized (committedIds) {
                        ids = new long[min(numEntries, committedIds.created.size())];
                        for (int i = 0; i < ids.length; i++) {
                            ids[i] = committedIds.created.removeAtIndex(random.nextInt(committedIds.created.size()));
                        }
                    }
                    try (var writer = tree.writer(NULL_CONTEXT)) {
                        for (long id : ids) {
                            writer.remove(layout.key(id));
                        }
                    }
                    synchronized (committedIds) {
                        committedIds.deleted.addAll(ids);
                    }
                }
                numCompletedTasks.add(1);
            }));
            race.addContestant(throwing(() -> {
                Thread.sleep(ThreadLocalRandom.current().nextInt(maxCheckpointDelay));
                tree.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
            }));
            race.goUnchecked();

            // then
            consistencyCheckStrict(tree);
            try (var seek = allEntriesSeek(tree, layout)) {
                for (long id : committedIds.created.toSortedArray()) {
                    assertThat(seek.next()).isTrue();
                    assertThat(layout.compare(seek.key(), layout.key(id))).isZero();
                }
                assertThat(seek.next()).isFalse();
            }
        }
    }

    private Seeker<KEY, VALUE> allEntriesSeek(GBPTree<KEY, VALUE> index, Layout<KEY, VALUE> layout) throws IOException {
        KEY low = layout.newKey();
        KEY high = layout.newKey();
        layout.initializeAsLowest(low);
        layout.initializeAsHighest(high);
        return index.seek(low, high, NULL_CONTEXT);
    }

    private static class Ids {
        private final MutableLongList created = LongLists.mutable.empty();
        private final MutableLongList deleted = LongLists.mutable.empty();
    }
}
