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
package org.neo4j.io.pagecache.tracing.linear;

import static org.neo4j.io.pagecache.tracing.linear.HEvents.EvictionRunHEvent;
import static org.neo4j.io.pagecache.tracing.linear.HEvents.FileFlushHEvent;
import static org.neo4j.io.pagecache.tracing.linear.HEvents.MappedFileHEvent;
import static org.neo4j.io.pagecache.tracing.linear.HEvents.UnmappedFileHEvent;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFileSwapperTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

/**
 * Tracer for global page cache events that add all of them to event history tracer that can build proper linear
 * history across all tracers.
 * Only use this for debugging internal data race bugs and the like, in the page cache.
 * @see HEvents
 * @see LinearHistoryPageCursorTracer
 */
public final class LinearHistoryPageCacheTracer implements PageCacheTracer {

    private final LinearHistoryTracer tracer;

    LinearHistoryPageCacheTracer(LinearHistoryTracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public PageFileSwapperTracer createFileSwapperTracer() {
        return PageFileSwapperTracer.NULL;
    }

    @Override
    public PageCursorTracer createPageCursorTracer(String tag) {
        return new LinearHistoryPageCursorTracer(tracer, tag);
    }

    @Override
    public void mappedFile(int swapperId, PagedFile pagedFile) {
        tracer.add(new MappedFileHEvent(pagedFile.path()));
    }

    @Override
    public void unmappedFile(int swapperId, PagedFile pagedFile) {
        tracer.add(new UnmappedFileHEvent(pagedFile.path()));
    }

    @Override
    public EvictionRunEvent beginPageEvictions(int pageCountToEvict) {
        return tracer.add(new EvictionRunHEvent(tracer, pageCountToEvict));
    }

    @Override
    public EvictionRunEvent beginEviction() {
        return tracer.add(new EvictionRunHEvent(tracer, 0));
    }

    @Override
    public FileFlushEvent beginFileFlush(PageSwapper swapper) {
        return tracer.add(new FileFlushHEvent(tracer, swapper.path()));
    }

    @Override
    public FileFlushEvent beginFileFlush() {
        return tracer.add(new FileFlushHEvent(tracer, null));
    }

    @Override
    public DatabaseFlushEvent beginDatabaseFlush() {
        return DatabaseFlushEvent.NULL;
    }

    @Override
    public long faults() {
        return 0;
    }

    @Override
    public long failedFaults() {
        return 0;
    }

    @Override
    public long noFaults() {
        return 0;
    }

    @Override
    public long vectoredFaults() {
        return 0;
    }

    @Override
    public long failedVectoredFaults() {
        return 0;
    }

    @Override
    public long noPinFaults() {
        return 0;
    }

    @Override
    public long evictions() {
        return 0;
    }

    @Override
    public long cooperativeEvictions() {
        return 0;
    }

    @Override
    public long pins() {
        return 0;
    }

    @Override
    public long unpins() {
        return 0;
    }

    @Override
    public long hits() {
        return 0;
    }

    @Override
    public long flushes() {
        return 0;
    }

    @Override
    public long evictionFlushes() {
        return 0;
    }

    @Override
    public long cooperativeEvictionFlushes() {
        return 0;
    }

    @Override
    public long merges() {
        return 0;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    public long bytesWritten() {
        return 0;
    }

    @Override
    public long bytesTruncated() {
        return 0;
    }

    @Override
    public long filesMapped() {
        return 0;
    }

    @Override
    public long filesUnmapped() {
        return 0;
    }

    @Override
    public long filesTruncated() {
        return 0;
    }

    @Override
    public long evictionExceptions() {
        return 0;
    }

    @Override
    public double hitRatio() {
        return 0d;
    }

    @Override
    public long maxPages() {
        return 0;
    }

    @Override
    public long iopqPerformed() {
        return 0;
    }

    @Override
    public long ioLimitedTimes() {
        return 0;
    }

    @Override
    public long ioLimitedMillis() {
        return 0;
    }

    @Override
    public long openedCursors() {
        return 0;
    }

    @Override
    public long closedCursors() {
        return 0;
    }

    @Override
    public long copiedPages() {
        return 0;
    }

    @Override
    public long snapshotsLoaded() {
        return 0;
    }

    @Override
    public void pins(long pins) {}

    @Override
    public void unpins(long unpins) {}

    @Override
    public void hits(long hits) {}

    @Override
    public void faults(long faults) {}

    @Override
    public void noFaults(long noFaults) {}

    @Override
    public void failedFaults(long failedFaults) {}

    @Override
    public void vectoredFaults(long faults) {}

    @Override
    public void failedVectoredFaults(long failedFaults) {}

    @Override
    public void noPinFaults(long faults) {}

    @Override
    public void bytesRead(long bytesRead) {}

    @Override
    public void evictions(long evictions) {}

    @Override
    public void cooperativeEvictions(long evictions) {}

    @Override
    public void cooperativeEvictionFlushes(long evictionFlushes) {}

    @Override
    public void evictionExceptions(long evictionExceptions) {}

    @Override
    public void bytesWritten(long bytesWritten) {}

    @Override
    public void flushes(long flushes) {}

    @Override
    public void merges(long merges) {}

    @Override
    public void snapshotsLoaded(long snapshotsLoaded) {}

    @Override
    public void maxPages(long maxPages, long pageSize) {}

    @Override
    public void iopq(long iopq) {}

    @Override
    public void limitIO(long millis) {}

    @Override
    public void pagesCopied(long copiesCreated) {}

    @Override
    public void filesTruncated(long truncatedFiles) {}

    @Override
    public void bytesTruncated(long bytesTruncated) {}

    @Override
    public void openedCursors(long openedCursors) {}

    @Override
    public void closedCursors(long closedCursors) {}

    @Override
    public void failedUnmap(String reason) {}
}
