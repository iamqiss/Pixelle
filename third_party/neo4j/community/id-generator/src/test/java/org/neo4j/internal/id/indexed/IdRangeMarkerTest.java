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
package org.neo4j.internal.id.indexed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.internal.id.indexed.IdRange.IdState.DELETED;
import static org.neo4j.internal.id.indexed.IdRange.IdState.FREE;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.IDS_PER_ENTRY;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.ValueHolder;
import org.neo4j.index.internal.gbptree.ValueMerger;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.id.TestIdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
@ExtendWith(RandomExtension.class)
class IdRangeMarkerTest {
    private static final IdRangeMerger MERGER = new IdRangeMerger(false, NO_MONITOR, null);

    @Inject
    PageCache pageCache;

    @Inject
    TestDirectory directory;

    @Inject
    FileSystemAbstraction fileSystem;

    @Inject
    RandomSupport random;

    private final int idsPerEntry = 128;
    private final IdRangeLayout layout = new IdRangeLayout(idsPerEntry);
    private final AtomicLong highestWritternId = new AtomicLong();
    private GBPTree<IdRangeKey, IdRange> tree;

    @BeforeEach
    void instantiateTree() {
        this.tree = new GBPTreeBuilder<>(pageCache, fileSystem, directory.file("file.id"), layout).build();
    }

    @AfterEach
    void close() throws IOException {
        tree.close();
    }

    @Test
    void shouldCreateEntryOnFirstAddition() throws IOException {
        // given
        ValueMerger merger = mock(ValueMerger.class);

        // when
        try (IdRangeMarker marker = instantiateMarker(mock(Lock.class), merger)) {
            marker.markDeleted(0);
        }

        // then
        verify(merger).added(any(), any());
        verify(merger).completed();
        verifyNoMoreInteractions(merger);
        try (Seeker<IdRangeKey, IdRange> seek = tree.seek(new IdRangeKey(0), new IdRangeKey(1), NULL_CONTEXT)) {
            assertTrue(seek.next());
            assertEquals(0, seek.key().getIdRangeIdx());
        }
    }

    @Test
    void shouldMergeAdditionIntoExistingEntry() throws IOException {
        // given
        try (IdRangeMarker marker = instantiateMarker(mock(Lock.class), mock(ValueMerger.class))) {
            marker.markDeleted(0);
        }

        // when
        ValueMerger merger = realMergerMock();
        try (IdRangeMarker marker = instantiateMarker(mock(Lock.class), merger)) {
            marker.markDeleted(1);
        }

        // then
        verify(merger).merge(any(), any(), any(), any());
        try (Seeker<IdRangeKey, IdRange> seek = tree.seek(new IdRangeKey(0), new IdRangeKey(1), NULL_CONTEXT)) {
            assertTrue(seek.next());
            assertEquals(0, seek.key().getIdRangeIdx());
            assertEquals(IdRange.IdState.DELETED, seek.value().getState(0));
            assertEquals(IdRange.IdState.DELETED, seek.value().getState(1));
            assertEquals(IdRange.IdState.USED, seek.value().getState(2));
        }
    }

    @Test
    void shouldNotCreateEntryOnFirstRemoval() throws IOException {
        // when
        ValueMerger merger = mock(ValueMerger.class);
        try (IdRangeMarker marker = instantiateMarker(mock(Lock.class), merger)) {
            marker.markUsed(0);
        }

        // then
        verify(merger).completed();
        verifyNoMoreInteractions(merger);
        try (Seeker<IdRangeKey, IdRange> seek =
                tree.seek(new IdRangeKey(0), new IdRangeKey(Long.MAX_VALUE), NULL_CONTEXT)) {
            assertFalse(seek.next());
        }
    }

    @Test
    void shouldRemoveEntryOnLastRemoval() throws IOException {
        // given
        int ids = 5;
        try (IdRangeMarker marker = instantiateMarker(mock(Lock.class), MERGER)) {
            for (long id = 0; id < ids; id++) {
                // let the id go through the desired states
                marker.markUsed(id);
                marker.markDeleted(id);
                marker.markFree(id);
                marker.markReserved(id);
            }
        }
        AtomicBoolean exists = new AtomicBoolean();
        tree.visit(
                new GBPTreeVisitor.Adaptor<>() {
                    @Override
                    public void key(IdRangeKey key, boolean isLeaf, long offloadId) {
                        if (isLeaf) {
                            assertEquals(0, key.getIdRangeIdx());
                            exists.set(true);
                        }
                    }
                },
                NULL_CONTEXT);

        // when
        try (IdRangeMarker marker = instantiateMarker(mock(Lock.class), MERGER)) {
            for (long id = 0; id < ids; id++) {
                marker.markUsed(id);
            }
        }

        // then
        tree.visit(
                new GBPTreeVisitor.Adaptor<>() {
                    @Override
                    public void key(IdRangeKey key, boolean isLeaf, long offloadId) {
                        assertFalse(isLeaf, "Should not have any key still in the tree, but got: " + key);
                    }
                },
                NULL_CONTEXT);
    }

    @Test
    void shouldUnlockOnCloseIfLockPresent() throws IOException {
        // given
        Lock lock = mock(Lock.class);

        // when
        try (IdRangeMarker marker = instantiateMarker(lock, mock(ValueMerger.class))) {
            verifyNoMoreInteractions(lock);
        }

        // then
        verify(lock).unlock();
    }

    @Test
    void shouldHandleCloseIfLockAbsent() throws IOException {
        // when
        var idRangeMarker = instantiateMarker(null, mock(ValueMerger.class));

        // then
        assertDoesNotThrow(idRangeMarker::close);
    }

    @Test
    void shouldCloseWriterOnClose() throws IOException {
        // when
        Writer writer = mock(Writer.class);
        try (IdRangeMarker marker = new IdRangeMarker(
                TestIdType.TEST,
                idsPerEntry,
                layout,
                writer,
                mock(Lock.class),
                mock(ValueMerger.class),
                true,
                new AtomicInteger(),
                1,
                new AtomicLong(-1),
                true,
                false,
                NO_MONITOR)) {
            verify(writer, never()).close();
        }

        // then
        verify(writer).close();
    }

    @Test
    void shouldIgnoreReservedIdsIfIdTypeSaysSo() throws IOException {
        // given
        long reservedId = IdValidator.INTEGER_MINUS_ONE;

        // when
        MutableLongSet expectedIds = LongSets.mutable.empty();
        try (IdRangeMarker marker = new IdRangeMarker(
                TestIdType.TEST,
                idsPerEntry,
                layout,
                tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT),
                mock(Lock.class),
                MERGER,
                true,
                new AtomicInteger(),
                1,
                new AtomicLong(reservedId - 1),
                true,
                false,
                NO_MONITOR)) {
            for (long id = reservedId - 1; id <= reservedId + 1; id++) {
                marker.markDeleted(id);
                if (id != reservedId) {
                    expectedIds.add(id);
                }
            }
        }

        // then
        assertEquals(expectedIds, gatherIds(DELETED));
    }

    @Test
    void shouldUseReservedIdsIfIdTypeSaysSo() throws IOException {
        // given
        long reservedId = IdValidator.INTEGER_MINUS_ONE;

        // when
        MutableLongSet expectedIds = LongSets.mutable.empty();
        try (IdRangeMarker marker = new IdRangeMarker(
                TestIdType.TEST_USING_RESERVED,
                idsPerEntry,
                layout,
                tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT),
                mock(Lock.class),
                MERGER,
                true,
                new AtomicInteger(),
                1,
                new AtomicLong(reservedId - 1),
                true,
                false,
                NO_MONITOR)) {
            for (long id = reservedId - 1; id <= reservedId + 1; id++) {
                marker.markDeleted(id);
                expectedIds.add(id);
            }
        }

        // then
        assertEquals(expectedIds, gatherIds(DELETED));
    }

    @Test
    void shouldBatchWriteIdBridging() {
        // given
        Writer<IdRangeKey, IdRange> writer = mock(Writer.class);
        try (IdRangeMarker marker = new IdRangeMarker(
                TestIdType.TEST,
                idsPerEntry,
                layout,
                writer,
                mock(Lock.class),
                MERGER,
                true,
                new AtomicInteger(),
                1,
                new AtomicLong(-1),
                true,
                false,
                NO_MONITOR)) {
            // when
            marker.markUsed(IDS_PER_ENTRY * 3);
        }

        // then
        // for the id bridging (one time for ids 0-9)
        verify(writer, times(3)).merge(any(), any(), any());
        // for the used bit update
        verify(writer, times(1)).mergeIfExists(any(), any(), any());
    }

    @ParameterizedTest
    @MethodSource("markOperations")
    void shouldBatchWriteIdsInSameRange(NamedOperation markOperation) {
        // given
        Writer<IdRangeKey, IdRange> writer = mock(Writer.class);
        // Marking as unallocated may bridge IDs w/o overwriting the "commit" bits
        // and therefore writes those separately. Get that out of the way for this test.
        int highestWrittenId = markOperation.name.equals("unallocated") ? 100 : -1;
        try (IdRangeMarker marker = new IdRangeMarker(
                TestIdType.TEST,
                idsPerEntry,
                layout,
                writer,
                mock(Lock.class),
                MERGER,
                true,
                new AtomicInteger(),
                1,
                new AtomicLong(highestWrittenId),
                true,
                false,
                NO_MONITOR)) {
            // when
            long id = 0;
            for (int i = 0; i < 10; i++) {
                int numberOfIds = random.nextInt(1, 5);
                markOperation.operation.apply(marker, id, numberOfIds);
                id += numberOfIds;
            }
        }

        // then
        if (markOperation.name.equals("used")) {
            verify(writer, times(1)).mergeIfExists(any(), any(), any());
        } else {
            verify(writer, times(1)).merge(any(), any(), any());
        }
    }

    @Test
    void shouldMarkDeletedAndFree() throws IOException {
        // given
        var freeIdsNotifier = new AtomicInteger();
        try (var marker = new IdRangeMarker(
                TestIdType.TEST,
                idsPerEntry,
                layout,
                tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT),
                mock(Lock.class),
                MERGER,
                true,
                freeIdsNotifier,
                1,
                new AtomicLong(-1),
                true,
                false,
                NO_MONITOR)) {
            // when
            marker.markDeletedAndFree(5, 3);
        }

        // then
        assertThat(freeIdsNotifier.get()).isGreaterThan(0);
        assertThat(gatherIds(IdRange.IdState.FREE)).isEqualTo(LongSets.immutable.of(5, 6, 7));
    }

    @Test
    void shouldSupportMarkUnallocatedWithLargerNumIdsThanRange() throws IOException {
        // given
        int numIdsBefore = 10;
        int numUnallocated = IDS_PER_ENTRY * 3;
        long highId = numIdsBefore + numUnallocated;
        try (var marker = new IdRangeMarker(
                TestIdType.TEST,
                idsPerEntry,
                layout,
                tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT),
                mock(Lock.class),
                MERGER,
                true,
                new AtomicInteger(),
                1,
                new AtomicLong(-1),
                true,
                false,
                NO_MONITOR)) {
            // when
            marker.markUnallocated(numIdsBefore, numUnallocated);
        }

        // then
        try (var seek = tree.seek(new IdRangeKey(0), new IdRangeKey(Long.MAX_VALUE), NULL_CONTEXT)) {
            while (seek.next()) {
                var range = seek.value();
                long id = seek.key().getIdRangeIdx() * IDS_PER_ENTRY;
                for (int i = 0; i < IDS_PER_ENTRY && id < highId; i++, id++) {
                    var expectedState = id < numIdsBefore ? DELETED : FREE;
                    assertThat(range.getState(i)).isEqualTo(expectedState);
                }
            }
        }
    }

    private static ValueMerger realMergerMock() {
        ValueMerger merger = mock(ValueMerger.class);
        when(merger.merge(any(), any(), any(), any()))
                .thenAnswer(invocation -> MERGER.merge(
                        invocation.getArgument(0),
                        invocation.getArgument(1),
                        invocation.getArgument(2),
                        invocation.getArgument(3)));
        return merger;
    }

    private IdRangeMarker instantiateMarker(Lock lock, ValueMerger merger) throws IOException {
        return new IdRangeMarker(
                TestIdType.TEST,
                idsPerEntry,
                layout,
                tree.writer(W_BATCHED_SINGLE_THREADED, NULL_CONTEXT),
                lock,
                merger,
                true,
                new AtomicInteger(),
                1,
                highestWritternId,
                true,
                false,
                NO_MONITOR);
    }

    private LongSet gatherIds(IdRange.IdState state) throws IOException {
        MutableLongSet deletedIdsInTree = LongSets.mutable.empty();
        tree.visit(
                new GBPTreeVisitor.Adaptor<>() {
                    private IdRangeKey idRangeKey;

                    @Override
                    public void key(IdRangeKey idRangeKey, boolean isLeaf, long offloadId) {
                        this.idRangeKey = idRangeKey;
                    }

                    @Override
                    public void value(ValueHolder<IdRange> idRange) {
                        for (int i = 0; i < idsPerEntry; i++) {
                            if (idRange.value.getState(i) == state) {
                                deletedIdsInTree.add(idRangeKey.getIdRangeIdx() * idsPerEntry + i);
                            }
                        }
                    }
                },
                NULL_CONTEXT);
        return deletedIdsInTree;
    }

    private interface Operation {
        void apply(IdRangeMarker marker, long id, int numberOfIds);
    }

    private record NamedOperation(String name, Operation operation) {
        @Override
        public String toString() {
            return name;
        }
    }

    public static Stream<Arguments> markOperations() {
        List<Arguments> operations = new ArrayList<>();
        operations.add(arguments(new NamedOperation("used", IdRangeMarker::markUsed)));
        operations.add(arguments(new NamedOperation("deleted", IdRangeMarker::markDeleted)));
        operations.add(arguments(new NamedOperation("deletedAndFree", IdRangeMarker::markDeletedAndFree)));
        operations.add(arguments(new NamedOperation("free", IdRangeMarker::markFree)));
        operations.add(arguments(new NamedOperation("reserved", IdRangeMarker::markReserved)));
        operations.add(arguments(new NamedOperation("unreserved", IdRangeMarker::markUnreserved)));
        operations.add(arguments(new NamedOperation("uncached", IdRangeMarker::markUncached)));
        operations.add(arguments(new NamedOperation("unallocated", IdRangeMarker::markUnallocated)));
        return operations.stream();
    }
}
