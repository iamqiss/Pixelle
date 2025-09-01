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
package org.neo4j.csv.reader;

import java.io.IOException;
import org.neo4j.csv.reader.Source.Chunk;

/**
 * In a scenario where there's one reader reading chunks of data, handing those chunks to one or
 * more processors (parsers) of that data, this class comes in handy. This pattern allows for
 * multiple {@link BufferedCharSeeker seeker instances}, each operating over one chunk, not transitioning itself
 * into the next.
 */
public abstract class NewLineChunker extends CharReadableChunker {

    private final HeaderSkipper headerSkip;
    private String lastSeenSourceDescription;
    private int fileIndex = -1;
    private long totalBackCopied;

    protected NewLineChunker(CharReadable reader, int chunkSize, HeaderSkipper headerSkip) {
        super(reader, chunkSize);
        this.headerSkip = headerSkip;
    }

    /**
     * @param buffer the character buffer to scan for the end of a row
     * @return the position of the end of the last row in the buffer
     */
    protected abstract int offsetOfLastRow(char[] buffer);

    /**
     * Fills the given chunk with data from the underlying {@link CharReadable}, up to a good cut-off point
     * in the vicinity of the buffer size.
     *
     * @param chunk {@link Chunk} to read data into.
     * @return the next {@link Chunk} of data, ending with a new-line or not for the last chunk.
     * @throws IOException on reading error.
     */
    @Override
    public synchronized boolean nextChunk(Chunk chunk) throws IOException {
        final var into = (ChunkImpl) chunk;
        final var chunkBuffer = into.buffer;
        var offset = fillFromBackBuffer(chunkBuffer);
        final var leftToRead = chunkSize - offset;
        var read = reader.read(chunkBuffer, offset, leftToRead);
        var emit = Emit.NO;
        if (read > 0) {
            if (read == leftToRead) {
                // Read from reader. We read data into the whole buffer and there seems to be more data left in reader.
                // This means we're most likely not at the end so seek backwards to the last newline character and
                // put the characters after the newline character(s) into the back buffer.
                final var newlineOffset = offsetOfLastRow(chunkBuffer) + 1;
                final var backCopied = storeInBackBuffer(chunkBuffer, newlineOffset, chunkSize - newlineOffset);
                totalBackCopied += backCopied;
                read -= backCopied;
                emit = Emit.YES_WITH_BACK_COPY;
            } else {
                emit = Emit.YES_NO_BACK_COPY;
            }

            offset += read;
        } else {
            // the check is for the case where the back-fill was exactly to the end of the input but as we back-filled
            // there is some data still to emit (offset ~= the amount from the back buffer and is reset after filling)
            if (offset > 0) {
                emit = Emit.YES_ONLY_BACK_FILLED;
            }
        }

        boolean newSource = crossedOverToNewSource();
        if (emit.send) {
            if (emit.updateWithRead) {
                position += read;
            }
            if (emit.updateWithBackCopied) {
                position += totalBackCopied;
                totalBackCopied = 0;
            }

            final var skipped = newSource && fileIndex >= 0 ? headerSkip.skipHeader(chunkBuffer, 0, offset) : 0;
            into.initialize(skipped, offset - skipped, lastSeenSourceDescription);
            return true;
        }
        return false;
    }

    private boolean crossedOverToNewSource() {
        final var currentSourceDescription = reader.sourceDescription();
        if (!currentSourceDescription.equals(lastSeenSourceDescription)) {
            fileIndex++;
            lastSeenSourceDescription = currentSourceDescription;
            return true;
        }
        return false;
    }

    private enum Emit {
        YES_WITH_BACK_COPY(true, true, false),
        YES_NO_BACK_COPY(true, true, true),
        YES_ONLY_BACK_FILLED(true, false, true),
        NO(false, false, false);

        private final boolean send;
        private final boolean updateWithRead;
        private final boolean updateWithBackCopied;

        Emit(boolean send, boolean updateWithRead, boolean updateWithBackCopied) {
            this.send = send;
            this.updateWithRead = updateWithRead;
            this.updateWithBackCopied = updateWithBackCopied;
        }
    }
}
