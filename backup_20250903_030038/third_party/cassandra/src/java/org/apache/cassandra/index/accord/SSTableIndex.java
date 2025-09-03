/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.accord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import accord.local.MaxDecidedRX.DecidedRX;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.index.accord.CheckpointIntervalArrayIndex.SegmentSearcher;
import org.apache.cassandra.index.accord.IndexDescriptor.IndexComponent;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

public class SSTableIndex extends SharedCloseableImpl
{
    public final IndexDescriptor id;
    private final Map<IndexComponent, FileHandle> files;
    private final List<Segment> segments;

    private SSTableIndex(IndexDescriptor id,
                         Map<IndexComponent, FileHandle> files,
                         List<Segment> segments,
                         Cleanup cleanup)
    {
        super(cleanup);
        this.id = id;
        this.files = files;
        this.segments = segments;
    }

    public SSTableIndex(SSTableIndex copy)
    {
        super(copy);
        this.id = copy.id;
        this.files = copy.files;
        this.segments = copy.segments;
    }

    @Override
    public SSTableIndex sharedCopy()
    {
        return new SSTableIndex(this);
    }

    public static SSTableIndex create(IndexDescriptor id) throws IOException
    {
        Map<IndexComponent, FileHandle> files = new EnumMap<>(IndexComponent.class);
        for (IndexComponent c : id.getLiveComponents())
            files.put(c, new FileHandle.Builder(id.fileFor(c)).mmapped(true).complete());
        List<Segment> segments = RouteIndexFormat.readSegments(files);
        files.remove(IndexComponent.SEGMENT).close();
        files.remove(IndexComponent.METADATA).close();
        Cleanup cleanup = new Cleanup(files);
        return new SSTableIndex(id, files, segments, cleanup);
    }

    public void search(Key group, byte[] key, TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX, Consumer<ByteBuffer> onMatch)
    {
        List<Segment> matches = segments.stream().filter(s -> {
                                            Segment.Metadata metadata = s.groups.get(group);
                                            if (metadata == null) return false;
                                            if (metadata.maxTxnId.compareTo(minTxnId) < 0 || metadata.minTxnId.compareTo(maxTxnId) > 0)
                                                return false;
                                            if (!RouteIndexFormat.includeByDecidedRX(decidedRX, metadata.maxRxId))
                                                return false;
                                            return ByteArrayUtil.compareUnsigned(metadata.minTerm, key) < 0
                                                   && ByteArrayUtil.compareUnsigned(metadata.maxTerm, key) >= 0;
                                        })
                                        .collect(Collectors.toList());
        if (matches.isEmpty()) return;
        for (Segment s : matches)
            search(s, group, key, onMatch);
    }

    private void search(Segment segment, Key group, byte[] key, Consumer<ByteBuffer> onMatch)
    {
        Segment.Metadata metadata = Objects.requireNonNull(segment.groups.get(group), () -> "Unknown group: " + group);
        try
        {
            SegmentSearcher searcher = new SegmentSearcher(fileFor(IndexComponent.CINTIA_SORTED_LIST), metadata.metas.get(IndexComponent.CINTIA_SORTED_LIST).offset,
                                                           fileFor(IndexComponent.CINTIA_CHECKPOINTS), metadata.metas.get(IndexComponent.CINTIA_CHECKPOINTS).offset);
            searcher.contains(key, interval -> onMatch.accept(ByteBuffer.wrap(interval.value)));
        }
        catch (IOException e)
        {
            throw new FSReadError(e, id.fileFor(IndexComponent.CINTIA_SORTED_LIST));
        }
    }

    public void search(Key group, byte[] start, byte[] end, TxnId minTxnId, Timestamp maxTxnId, @Nullable DecidedRX decidedRX, Consumer<ByteBuffer> onMatch)
    {
        List<Segment> matches = segments.stream().filter(s -> {
                                            Segment.Metadata metadata = s.groups.get(group);
                                            if (metadata == null) return false;
                                            if (metadata.maxTxnId.compareTo(minTxnId) < 0 || metadata.minTxnId.compareTo(maxTxnId) > 0)
                                                return false;
                                            if (!RouteIndexFormat.includeByDecidedRX(decidedRX, metadata.maxRxId))
                                                return false;
                                            if (ByteArrayUtil.compareUnsigned(metadata.minTerm, end) >= 0)
                                                return false;
                                            if (ByteArrayUtil.compareUnsigned(metadata.maxTerm, start) <= 0)
                                                return false;
                                            return true;
                                        })
                                        .collect(Collectors.toList());
        if (matches.isEmpty()) return;
        for (Segment s : matches)
            search(s, group, start, end, onMatch);
    }

    private void search(Segment segment, Key group, byte[] start, byte[] end, Consumer<ByteBuffer> onMatch)
    {
        Segment.Metadata metadata = Objects.requireNonNull(segment.groups.get(group), () -> "Unknown group: " + group);
        try
        {
            SegmentSearcher searcher = new SegmentSearcher(fileFor(IndexComponent.CINTIA_SORTED_LIST), metadata.metas.get(IndexComponent.CINTIA_SORTED_LIST).offset,
                                                           fileFor(IndexComponent.CINTIA_CHECKPOINTS), metadata.metas.get(IndexComponent.CINTIA_CHECKPOINTS).offset);
            searcher.intersects(start, end, interval -> onMatch.accept(ByteBuffer.wrap(interval.value)));
        }
        catch (IOException e)
        {
            throw new FSReadError(e, id.fileFor(IndexComponent.CINTIA_SORTED_LIST));
        }
    }

    private FileHandle fileFor(IndexComponent c)
    {
        return Objects.requireNonNull(files.get(c), () -> "Unknown component: " + c);
    }

    private static class Cleanup implements RefCounted.Tidy
    {
        private final Map<IndexComponent, FileHandle> files;

        private Cleanup(Map<IndexComponent, FileHandle> files)
        {
            this.files = files;
        }

        @Override
        public void tidy() throws Exception
        {
            for (IndexComponent c : IndexComponent.values())
            {
                FileHandle fh = files.remove(c);
                if (fh == null) continue;
                fh.close();
            }
        }

        @Override
        public String name()
        {
            return "SSTableIndex Cleanup";
        }
    }
}
