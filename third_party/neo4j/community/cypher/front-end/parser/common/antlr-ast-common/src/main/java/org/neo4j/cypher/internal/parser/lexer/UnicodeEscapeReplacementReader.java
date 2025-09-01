/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.parser.lexer;

import static java.lang.Character.isHighSurrogate;
import static java.lang.Character.isLowSurrogate;
import static java.lang.Math.min;
import static org.antlr.v4.runtime.CodePointCharStream.fromBuffer;
import static org.antlr.v4.runtime.IntStream.UNKNOWN_SOURCE_NAME;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CodePointBuffer;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.neo4j.cypher.internal.util.InputPosition;

/** Reader that replace cypher unicode escape codes and keep track of query offsets. */
public class UnicodeEscapeReplacementReader extends Reader {
    public static int DEFAULT_BUFFER_SIZE = 4096;
    private final String cypher;
    private int srcPos;
    private OffsetTableBuilder offsetTable = null;
    boolean escaped = false; // If true last character was a backslash

    private UnicodeEscapeReplacementReader(String cypher) {
        this.cypher = cypher;
    }

    public static Result read(String cypher) throws IOException {
        return read(cypher, DEFAULT_BUFFER_SIZE);
    }

    public static Result read(String cypher, int maxBuffer) throws IOException {
        final var antlrBuffer = CodePointBuffer.builder(cypher.length());
        final var cb = CharBuffer.allocate(min(maxBuffer, cypher.length()));

        try (final var reader = new UnicodeEscapeReplacementReader(cypher)) {
            while (reader.read(cb.clear()) != -1) antlrBuffer.append(cb.flip());
            final var charStream = fromBuffer(antlrBuffer.build(), UNKNOWN_SOURCE_NAME);
            return new Result(charStream, reader.offsetTable());
        }
    }

    @Override
    public int read(char[] cbuf, int off, int len) {
        if (srcPos >= cypher.length()) return -1;
        return doRead(cbuf, off, len);
    }

    private int doRead(char[] cbuf, int off, int len) {
        final String src = cypher;
        int srcPos = this.srcPos, srcEnd = src.length(), destPos = off, destEnd = off + len;
        boolean escaped = this.escaped;
        var offsetTable = this.offsetTable;
        while (srcPos < srcEnd && destPos < destEnd) {
            char c = src.charAt(srcPos);

            boolean isReplacement = !escaped && c == '\\' && peek(srcPos + 1) == 'u';
            if (isReplacement) c = parseUnicodeReplacement(srcPos);

            boolean isHighSurrogate = isHighSurrogate(c);
            // Antlr do not like cut off surrogates, so we do our best to avoid that here
            if (isHighSurrogate && destPos + 1 == destEnd && srcPos + 1 < srcEnd && len > 1) break;

            // If input positions in the result (relative to codepoints) is different from input positions
            // in the input (relative to chars) we need to update the offset table. Most queries don't need this.
            if (offsetTable != null || isReplacement || isHighSurrogate) offsetTable = updateOffsetTable(srcPos, c);

            escaped = !escaped && !isReplacement && c == '\\';
            cbuf[destPos++] = c;
            srcPos += isReplacement ? 6 : 1;
        }
        this.srcPos = srcPos;
        this.escaped = escaped;
        return destPos - off;
    }

    // Peeks the character at the specified offset or return 0(!) if none is found
    private char peek(int pos) {
        return pos < cypher.length() ? cypher.charAt(pos) : 0;
    }

    private OffsetTableBuilder updateOffsetTable(int charPos, char destChar) {
        if (offsetTable == null) {
            offsetTable = new OffsetTableBuilder(inputPositionAt(charPos), cypher.charAt(charPos), destChar);
        } else {
            offsetTable.updateOffsets(charPos, cypher.charAt(charPos), destChar);
        }
        return offsetTable;
    }

    private InputPosition inputPositionAt(int pos) {
        int line = 1, col = 1;
        for (int i = 0; i < pos && i < cypher.length(); ++i) {
            char c = cypher.charAt(i);
            if (c == '\n' || (c == '\r' && peek(i + 1) != '\n')) {
                line += 1;
                col = 0;
            }
            col += 1;
        }
        return InputPosition.apply(pos, line, col);
    }

    private char parseUnicodeReplacement(int charPos) {
        final var hexString = cypher.substring(min(charPos + 2, cypher.length()), min(charPos + 6, cypher.length()));
        try {
            return (char) Integer.parseInt(hexString, 16);
        } catch (Exception e) {
            var pos = inputPositionAt(charPos + 2);
            var m = "Invalid input '%s': expected four hexadecimal digits specifying a unicode character";
            throw new InvalidUnicodeLiteral(m.formatted(hexString), charPos, pos.line(), pos.column());
        }
    }

    public int[] offsetTable() {
        return offsetTable != null ? offsetTable.offsets.toArray() : null;
    }

    @Override
    public void close() {}

    private static class OffsetTableBuilder {
        IntArrayList offsets = new IntArrayList();
        char lastSrcChar, lastDestChar;

        OffsetTableBuilder(InputPosition start, char srcChar, char destChar) {
            this.offsets.addAll(start.offset(), start.line(), start.column());
            this.lastSrcChar = srcChar;
            this.lastDestChar = destChar;
        }

        void updateOffsets(int charPos, char srcChar, char destChar) {
            int line = offsets.get(offsets.size() - 2);
            int col = offsets.getLast() + charPos - offsets.get(offsets.size() - 3);
            if (lastSrcChar == '\n' || (lastSrcChar == '\r' && srcChar != '\n')) {
                line += 1;
                col = 1;
            }
            if (!(isHighSurrogate(lastDestChar) && isLowSurrogate(destChar))) offsets.addAll(charPos, line, col);
            lastSrcChar = srcChar;
            lastDestChar = destChar;
        }
    }

    public record Result(CharStream charStream, int[] offsetTable) {}

    public static class InvalidUnicodeLiteral extends RuntimeException {
        public final int offset, line, column;

        private InvalidUnicodeLiteral(String message, int offset, int line, int column) {
            super(message);
            this.offset = offset;
            this.line = line;
            this.column = column;
        }
    }
}
