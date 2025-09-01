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

import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.csv.reader.HeaderSkipper.NO_SKIP;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.csv.reader.CharReadableChunker.ChunkImpl;
import org.neo4j.csv.reader.Source.Chunk;
import org.neo4j.function.Predicates;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

class NewLineChunkerTest {

    private static final String UNIX_NL = "\n";
    private static final String WINDOWS_NL = "\r\n";

    private static final List<CsvConfig> CSV_CONFIGS = List.of(
            new CsvConfig(',', '"'),
            new CsvConfig('\t', '"'),
            new CsvConfig('|', '"'),
            new CsvConfig(',', '\''),
            new CsvConfig('\t', '\''),
            new CsvConfig('|', '\''));

    @ExtendWith(RandomExtension.class)
    private abstract static class BaseTest {

        @Inject
        protected RandomSupport random;

        private final boolean requiresQuotedCell;

        protected BaseTest(boolean requiresQuotedCell) {
            this.requiresQuotedCell = requiresQuotedCell;
        }

        protected abstract NewLineChunker createChunker(CharReadable readable, Configuration config);

        protected abstract Configuration.Builder configurationBuilder();

        @Test
        void parsingWhereBufferIsSlightlyBiggerThanRows() throws IOException {
            assertParsing(
                    random.among(CSV_CONFIGS),
                    lines -> lines.stream().mapToInt(String::length).max().orElseThrow() + random.nextInt(10, 100));
        }

        @Test
        void parsingWhereBufferIsBiggerThanRows() throws IOException {
            final var fudgeFactor = random.nextInt(2, 9);
            assertParsing(
                    random.among(CSV_CONFIGS),
                    lines -> lines.stream().mapToInt(String::length).max().orElseThrow() * fudgeFactor);
        }

        @Test
        void parsingWhereBufferCoversAllData() throws IOException {
            assertParsing(
                    random.among(CSV_CONFIGS),
                    lines -> lines.stream().mapToInt(String::length).sum() * 2);
        }

        @Test
        void parsingWhereBufferIsCloseToDataSize() throws IOException {
            assertParsing(
                    random.among(CSV_CONFIGS),
                    lines -> lines.stream().mapToInt(String::length).sum() + random.nextInt(-5, 5));
        }

        @Test
        void parsingWithMultipleSources() throws IOException {
            final var csvConfig = random.among(CSV_CONFIGS);
            final var nl = random.nextBoolean() ? UNIX_NL : WINDOWS_NL;

            final var csv1 = Csv.create(createImportData(csvConfig, nl), nl, true);
            final var csv2 = Csv.create(createImportData(csvConfig, nl), nl, true);

            final var configuration = configurationBuilder()
                    .withDelimiter(csvConfig.delimiter)
                    .withQuotationCharacter(csvConfig.quote)
                    .withBufferSize(Math.max(csv1.length(), csv2.length()) + random.nextInt(-10, 10))
                    .build();

            try (var source = createChunker(
                    new MultiReadable(Readables.iterator(Readables::wrap, csv1.data, csv2.data)), configuration)) {
                assertChunkData(source, Csv.merge(csv1, csv2));
            }
        }

        protected void assertParsing(CsvConfig csvConfig, Function<List<String>, Integer> bufferSize)
                throws IOException {
            final var nl = random.nextBoolean() ? UNIX_NL : WINDOWS_NL;
            final var lines = createImportData(csvConfig, nl);
            assertParsing(csvConfig, Csv.create(lines, nl), bufferSize.apply(lines));
        }

        protected void assertParsing(CsvConfig csvConfig, Csv csv, int bufferSize) throws IOException {
            final var configuration = configurationBuilder()
                    .withBufferSize(bufferSize)
                    .withQuotationCharacter(csvConfig.quote)
                    .withDelimiter(csvConfig.delimiter)
                    .build();
            try (var source = createChunker(Readables.wrap(csv.data), configuration)) {
                assertChunkData(source, csv);
            }
        }

        private List<String> createImportData(CsvConfig csvConfig, String nl) {
            final var rows = random.nextInt(10, 150);
            final var columns = random.nextInt(5, 42);
            final var multiLineFields = requiresQuotedCell ? random.nextInt(1, Math.min(columns - 1, 5)) : 0;

            final var lines = Lists.mutable.<String>ofInitialCapacity(rows);
            for (var row = 0; row < rows; row++) {
                var multiCount = multiLineFields;
                final var line = new StringBuilder().append(row + 1);
                for (var column = 1; column < columns; column++) {
                    line.append(csvConfig.delimiter);

                    if (multiCount > 0 && (columns - column >= multiCount || random.nextFloat() > 0.75f)) {
                        final var newLines = IntStream.range(2, 11)
                                .mapToObj(i -> randomText(random, csvConfig, true))
                                .collect(Collectors.joining(nl));
                        line.append(csvConfig.quote).append(newLines).append(csvConfig.quote);
                        multiCount--;
                    } else {
                        final var rv = random.nextFloat();
                        //noinspection StatementWithEmptyBody
                        if (rv < 0.05f) {
                            // skip to leave the empty cell
                        } else if (rv < 0.20f) {
                            if (requiresQuotedCell || random.nextBoolean()) {
                                line.append(csvConfig.quote)
                                        .append(randomText(random, csvConfig, true))
                                        .append(csvConfig.quote);
                            } else {
                                line.append(randomText(random, csvConfig, false));
                            }
                        } else {
                            line.append(random.nextInt());
                        }
                    }
                }

                lines.add(line.toString());
            }

            return lines;
        }

        private static String randomText(RandomSupport random, CsvConfig csvConfig, boolean allowsEscapes) {
            final var line = new StringBuilder();
            line.append(random.nextAlphaNumericString());
            if (random.nextFloat() > 0.50f) {
                final var offset = random.nextInt(line.length());
                if (random.nextFloat() > 0.50f) {
                    line.insert(offset, csvConfig.delimiter);
                } else if (allowsEscapes) {
                    // add some escaped quote characters, NB we support \" and ""
                    final var escapedChar = random.nextBoolean() ? '\\' : csvConfig.quote;
                    line.insert(offset, csvConfig.quote).insert(offset, escapedChar);
                }
            }
            return line.toString();
        }

        private static void assertChunkData(NewLineChunker source, Csv csv) throws IOException {
            final var data = csv.data.toCharArray();
            var pos = 0;
            var lineIndex = 0;
            final var chunk = source.newChunk();
            while (source.nextChunk(chunk)) {
                pos = assertChunk(data, chunk, pos);
                while (csv.lineEndings[lineIndex] < pos) {
                    lineIndex++;
                }
                assertThat(pos)
                        .as("should break the chunks at some line boundary")
                        .isEqualTo(csv.lineEndings[lineIndex]);
            }

            final var finalPos = pos;
            assertThat(finalPos)
                    .as(() -> "should have consumed all the data - found remaining:%n%s"
                            .formatted(new String(Arrays.copyOfRange(data, finalPos, csv.length()))))
                    .isEqualTo(csv.length());

            assertThat(source.position())
                    .as("source should have progressed to the end")
                    .isEqualTo(csv.length());
        }

        private static int assertChunk(char[] data, ChunkImpl chunk, int pos) {
            final var chunkChars = characters(chunk);

            final var nextPos = pos + chunkChars.length;
            assertThat(nextPos).isLessThanOrEqualTo(data.length);
            assertThat(chunkChars).isEqualTo(Arrays.copyOfRange(data, pos, nextPos));
            return nextPos;
        }

        protected static char[] characters(Chunk chunk) {
            return copyOfRange(chunk.data(), chunk.startPosition(), chunk.startPosition() + chunk.length());
        }
    }

    @Nested
    class MultiLine extends BaseTest {

        MultiLine() {
            super(true);
        }

        @Override
        protected Configuration.Builder configurationBuilder() {
            return Configuration.newBuilder().withMultilineDocuments(Predicates.alwaysTrue());
        }

        @Override
        protected NewLineChunker createChunker(CharReadable readable, Configuration config) {
            return new MultiLineChunker(readable, config, NO_SKIP);
        }

        @Test
        void shouldThrowWhenNoQuotedTextFieldsPresent() {
            final var csvConfig = new CsvConfig(',', '\'');
            final var csv = Csv.create(
                    List.of(
                            "1,a,b,c,d,e,f,this is some text",
                            "2,a,b,c,d,e,f,this is some longer text",
                            "3,a,b,c,d,e,f,this is some slightly longer text",
                            "4,a,b,c,d,e,f,this is some even longer text",
                            "5,a,b,c,d,e,f,this is some long text",
                            "6,a,b,c,d,e,f,this is some more long text",
                            "7,a,b,c,d,e,f,this is some slightly more long text",
                            "8,a,b,c,d,e,f,this is some even more long text",
                            "9,a,b,c,d,e,f,this is some much longer text that still doesn't have any quotes to be seen"),
                    UNIX_NL);

            final var bufferSize = csv.length() / 2;
            assertThatThrownBy(() -> assertParsing(csvConfig, csv, bufferSize))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContainingAll(
                            "Weird input data",
                            "reached beginning with no previous CR",
                            "at position 0 of buffer of length",
                            String.valueOf(bufferSize),
                            "not supported a.t.m.");
        }

        @Test
        void shouldThrowWhenBufferSizeIsTooSmall() {
            final var fudgeFactor = random.nextInt(10, 100);
            assertThatThrownBy(() -> assertParsing(
                            random.among(CSV_CONFIGS),
                            lines -> lines.stream()
                                            .mapToInt(String::length)
                                            .min()
                                            .orElseThrow()
                                    - fudgeFactor))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContainingAll(
                            "Weird input data", "reached beginning with no previous CR", "not supported a.t.m.");
        }

        @Test
        void parsingWhereNewLineIsLastCharacter() throws IOException {
            // found this scenario in a randomised test (multilineParsingWhereBufferIsBiggerThanRows
            // seed=1727885371883L)
            // and then tweaked it to get it simpler to debug
            final var nl = random.nextBoolean() ? UNIX_NL : WINDOWS_NL;
            final var csvConfig = new CsvConfig('|', '\'');

            final var csv = Csv.create(
                    List.of(
                            "1|'this'|'is'|'some'|'text'|'with a%snew line'|52|'bif'".formatted(nl),
                            "2|'this'|'is'|'some'|'text'|'with a%snew line'|52|'baf'".formatted(nl),
                            "13|'this'|'is'|'some'|'text'|'with another new line%s'|52|'bof'".formatted(nl)),
                    nl);

            assertParsing(csvConfig, csv, csv.length() - 6);
        }
    }

    @Nested
    class ClosestNewLine extends BaseTest {

        ClosestNewLine() {
            super(false);
        }

        @Override
        protected Configuration.Builder configurationBuilder() {
            return Configuration.newBuilder();
        }

        @Override
        protected NewLineChunker createChunker(CharReadable readable, Configuration config) {
            return new ClosestNewLineChunker(readable, config.bufferSize(), NO_SKIP);
        }

        @Test
        void shouldFailIfNoNewlineInChunk() throws Exception {
            final var lines = List.of("1234567", "89012345678901234");

            final var bufferSize = 12;
            final var config = configurationBuilder().withBufferSize(bufferSize).build();
            try (var source = createChunker(Readables.wrap(Csv.create(lines, UNIX_NL).data), config)) {
                final var chunk = source.newChunk();
                assertThat(source.nextChunk(chunk)).isTrue();
                assertThat(characters(chunk)).isEqualTo((lines.get(0) + UNIX_NL).toCharArray());
                assertThatThrownBy(() -> source.nextChunk(chunk))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContainingAll(
                                "Weird input data",
                                "no newline character in the whole buffer",
                                String.valueOf(bufferSize),
                                "not supported a.t.m.");
            }
        }
    }

    private record CsvConfig(char delimiter, char quote) {}

    private record Csv(String data, int... lineEndings) {
        private static Csv create(List<String> lines, String nl) {
            return create(lines, nl, false);
        }

        private static Csv create(List<String> lines, String nl, boolean forceTerminalNL) {
            var csv = String.join(nl, lines);
            if (forceTerminalNL) {
                csv += nl;
            }

            final var endings = IntLists.mutable.empty();
            var pos = 0;
            for (String line : lines) {
                pos += line.length();
                if (pos < csv.length() || csv.endsWith(nl)) {
                    pos += nl.length();
                }
                endings.add(pos);
            }
            return new Csv(csv, endings.toArray());
        }

        private static Csv merge(Csv csv1, Csv csv2) {
            final var lastEndingInCsv1 = csv1.lineEndings[csv1.lineEndings.length - 1];
            final var endings = IntLists.mutable.empty();
            endings.addAll(csv1.lineEndings);
            for (var ending : csv2.lineEndings) {
                endings.add(ending + lastEndingInCsv1);
            }

            return new Csv(csv1.data + csv2.data, endings.toArray());
        }

        private int length() {
            return data.length();
        }
    }
}
