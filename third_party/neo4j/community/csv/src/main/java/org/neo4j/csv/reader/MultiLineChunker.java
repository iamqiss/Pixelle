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

import java.util.LinkedList;
import java.util.OptionalInt;

/**
 * A chunker that can to determine the row boundary for content that allows multi-line rows, i.e. the row itself can
 * contain new line characters.
 * <br>
 * There is an assumption that <strong>at least one</strong> of the fields in a row will be 'quoted' via the
 * {@link Configuration#quotationCharacter()} setting.
 */
public class MultiLineChunker extends NewLineChunker {

    private final char delimiter;
    private final char quotationCharacter;

    public MultiLineChunker(CharReadable reader, Configuration config, HeaderSkipper headerSkip) {
        super(reader, config.bufferSize(), headerSkip);
        this.delimiter = config.delimiter();
        this.quotationCharacter = config.quotationCharacter();
    }

    @SuppressWarnings("DataFlowIssue")
    @Override
    protected int offsetOfLastRow(char[] buffer) {
        final var states = new LinkedList<ParseState>();
        states.push(ParseState.endOfBuffer(buffer));

        var pos = states.peek().offset;
        while (pos >= 0) {
            if (isEscapedQuote(buffer, pos)) {
                pos -= 2; // consume the quote and the escape/quote char
            } else if (isCellBoundary(buffer, pos)) {
                pos--; // consume the quote
                if (isDelimiter(buffer, pos)) {
                    states.push(ParseState.startText(pos + 1));
                    states.push(ParseState.delimiter(pos--)); // consume the delimiter
                    if (isCellBoundary(buffer, pos)) {
                        states.push(ParseState.endText(pos--)); // consume the quote
                    } else {
                        states.push(ParseState.endOthers(pos));
                    }
                } else {
                    switch (states.peek().state) {
                        case END_TEXT_CELL -> states.push(ParseState.startText(pos + 1));
                        case END_OTHER_CELLS -> states.push(ParseState.startOthers(pos + 1));
                            // first char read is the end of some text cell
                        case DELIMITER, EOB -> states.push(ParseState.endText(pos + 1));
                        case CR -> {
                            final var currentPos = pos;
                            return scanBackForCRAfterQuote(states)
                                    .orElseThrow(() -> error("reached floating quote", currentPos));
                        }
                        case START_OTHER_CELLS, START_TEXT_CELL -> throw error("reached floating quote", pos);
                    }
                }
            } else if (isDelimiter(buffer, pos)) {
                pos--; // consume the delimiter
                switch (states.peek().state) {
                    case EOB, CR -> states.push(ParseState.delimiter(pos + 1));
                    case END_OTHER_CELLS -> {
                        states.push(ParseState.startOthers(pos + 2));
                        states.push(ParseState.delimiter(pos + 1));
                        if (isCellBoundary(buffer, pos)) {
                            states.push(ParseState.endText(pos--)); // consume the quote
                        } else {
                            states.push(ParseState.endOthers(pos));
                        }
                    }
                    case START_TEXT_CELL, START_OTHER_CELLS -> {
                        states.push(ParseState.delimiter(pos + 1));
                        if (isCellBoundary(buffer, pos)) {
                            states.push(ParseState.endText(pos--)); // consume the quote
                        } else {
                            states.push(ParseState.endOthers(pos));
                        }
                    }
                }
                // END_TEXT_CELL is no-op as delimiter is part of text cells
            } else {
                final var cr = checkForCR(buffer, pos);
                if (cr == 0) {
                    pos--; // consume the char
                } else {
                    final var posAfterCR = pos;
                    pos -= cr; // consume the CR
                    switch (states.peek().state) {
                        case EOB -> states.push(ParseState.newline(posAfterCR));
                        case DELIMITER, CR -> ensureInTextCellForCR(states, posAfterCR);
                            // , or ," would already have consumed delimiter (and be in END_X state)
                        case START_TEXT_CELL, START_OTHER_CELLS -> throw error(
                                "found CR outside a text cell", posAfterCR);
                        case END_OTHER_CELLS -> {
                            // after a run of cells non text cells found the start of the row
                            return posAfterCR;
                        }
                    }
                    // END_TEXT_CELL is no-op as CR is part of text cells
                }
            }
        }

        return scanBackForCRAfterEnds(states).orElseThrow(() -> error("reached beginning with no previous CR", 0));
    }

    private static OptionalInt scanBackForCRAfterQuote(LinkedList<ParseState> states) {
        // this is for the case when search started just after a row starts but just before start of text cell
        // 1,a,b,"c"
        // 2
        // ^
        // OR
        // 1,a,b,"c"
        // 2,d
        //   ^
        if (states.size() >= 2) {
            final var iterator = states.iterator();
            var parseState = iterator.next();
            if (parseState.state == ReadState.CR) {
                final var crPos = parseState.offset;
                var mismatched = false;
                while (iterator.hasNext()) {
                    parseState = iterator.next();
                    if (!(parseState.state == ReadState.DELIMITER || parseState.state == ReadState.EOB)) {
                        mismatched = true;
                        break;
                    }
                }

                if (!mismatched) {
                    return OptionalInt.of(crPos);
                }
            }
        }
        return OptionalInt.empty();
    }

    private static OptionalInt scanBackForCRAfterEnds(LinkedList<ParseState> states) {
        // this is for the case when search started in row fragment with no quoted-text cells, i.e.
        // 1,a,b,"c",d
        // 2,e,f,"g",h
        //     ^
        // if we scanned back from the marker
        ReadState previous = null;
        for (var parseState : states) {
            final var state = parseState.state;
            if (state == ReadState.CR
                    && (previous == ReadState.END_OTHER_CELLS || previous == ReadState.END_TEXT_CELL)) {
                return OptionalInt.of(parseState.offset);
            }
            previous = state;
        }

        return OptionalInt.empty();
    }

    private void ensureInTextCellForCR(LinkedList<ParseState> states, int pos) {
        final var state = states.stream()
                .map(ParseState::state)
                .filter(s -> !(s == ReadState.CR || s == ReadState.DELIMITER))
                .findFirst()
                .orElseThrow(); // should always have at least EOB
        if (state == ReadState.END_TEXT_CELL || state == ReadState.EOB) {
            states.push(ParseState.newline(pos));
        } else {
            throw error("found CR outside of a text cell", pos);
        }
    }

    private IllegalStateException error(String message, int position) {
        return new IllegalStateException(
                "Weird input data, %s at position %d of buffer of length %d, not supported a.t.m."
                        .formatted(message, position, chunkSize));
    }

    private boolean isDelimiter(char[] buffer, int offset) {
        return buffer[offset] == delimiter;
    }

    private boolean isCellBoundary(char[] buffer, int offset) {
        if (buffer[offset] == quotationCharacter) {
            return offset == 0 || !isQuoteEscapeChar(buffer, offset - 1);
        }

        return false;
    }

    private boolean isEscapedQuote(char[] buffer, int offset) {
        if (offset > 0 && buffer[offset] == quotationCharacter) {
            return isQuoteEscapeChar(buffer, offset - 1);
        }

        return false;
    }

    private boolean isQuoteEscapeChar(char[] buffer, int offset) {
        return buffer[offset] == '\\' || buffer[offset] == quotationCharacter;
    }

    private static int checkForCR(char[] buffer, int offset) {
        if (buffer[offset] == '\n') {
            return offset == 0 || buffer[offset - 1] != '\r' ? 1 : 2;
        }

        // don't match for just '\r' as we could get a windows CR split across a chunk boundary
        // in this case, we'd want to keep searching to ensure we get the full CR in the next chunk
        return 0;
    }

    private record ParseState(int offset, ReadState state) {
        private static ParseState endOfBuffer(char[] buffer) {
            return new ParseState(buffer.length - 1, ReadState.EOB);
        }

        private static ParseState delimiter(int pos) {
            return new ParseState(pos, ReadState.DELIMITER);
        }

        private static ParseState newline(int pos) {
            return new ParseState(pos, ReadState.CR);
        }

        private static ParseState startText(int pos) {
            return new ParseState(pos, ReadState.START_TEXT_CELL);
        }

        private static ParseState endText(int pos) {
            return new ParseState(pos, ReadState.END_TEXT_CELL);
        }

        private static ParseState startOthers(int pos) {
            return new ParseState(pos, ReadState.START_OTHER_CELLS);
        }

        private static ParseState endOthers(int pos) {
            return new ParseState(pos, ReadState.END_OTHER_CELLS);
        }
    }

    private enum ReadState {
        EOB,
        DELIMITER,
        START_TEXT_CELL,
        END_TEXT_CELL,
        START_OTHER_CELLS,
        END_OTHER_CELLS,
        CR
    }
}
