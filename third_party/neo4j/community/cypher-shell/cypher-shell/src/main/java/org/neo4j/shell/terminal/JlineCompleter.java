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
package org.neo4j.shell.terminal;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.neo4j.shell.commands.CommandHelper.CommandFactoryHelper;
import org.neo4j.shell.completions.CompletionEngine;
import org.neo4j.shell.completions.Suggestion;
import org.neo4j.shell.completions.SuggestionType;
import org.neo4j.shell.terminal.StatementJlineParser.BlankCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.CommandCompletion;
import org.neo4j.shell.terminal.StatementJlineParser.CypherCompletion;

/**
 * Provides autocompletion for cypher shell statements.
 */
public class JlineCompleter implements Completer {
    private final CommandCompleter commandCompleter;
    private final CypherCompleter cypherCompleter;

    public JlineCompleter(CommandFactoryHelper commands, CompletionEngine completionEngine) {
        this.commandCompleter = CommandCompleter.from(commands);
        this.cypherCompleter = new CypherCompleter(completionEngine);
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        // Note, the JavaCC parser doesn't provide good enough completion for
        // us to release it yet. For this reason, cypher completion is
        // disabled by default for now until we decide exactly where we want to
        // go with this.
        try {
            if (line instanceof BlankCompletion) {
                candidates.addAll(commandCompleter.complete());
                if (cypherCompleter.completionsEnabled()) {
                    cypherCompleter.completeBlank().forEach(candidates::add);
                }
            } else if (line instanceof CommandCompletion) {
                candidates.addAll(commandCompleter.complete());
            } else if (cypherCompleter.completionsEnabled() && line instanceof CypherCompletion cypher) {
                cypherCompleter.complete(cypher).forEach(candidates::add);
            }
        } catch (Exception e) {
            // Ignore
        }
    }

    private record CommandCompleter(List<Suggestion> allCommands) {
        List<Suggestion> complete() {
            return allCommands;
        }

        public static CommandCompleter from(CommandFactoryHelper commands) {
            return new CommandCompleter(
                    commands.metadata().map(Suggestion::command).toList());
        }
    }

    private record CypherCompleter(CompletionEngine completionEngine) {
        public boolean completionsEnabled() {
            return completionEngine.completionsEnabled();
        }

        Stream<Suggestion> complete(CypherCompletion cypher) throws IOException {
            return concat(completions(queryUntilCompletionWord(cypher), cypher.word()));
        }

        Stream<Suggestion> completeBlank() throws IOException {
            return concat(completions("", ""));
        }

        private String textUntilSentinel(String lastToken, char sentinel) {
            var sentinelIndex = lastToken.lastIndexOf(sentinel);
            return lastToken.substring(0, sentinelIndex + 1);
        }

        /*
         * Returns cypher keyword suggestions, for example `MATCH`.
         */
        private Stream<Suggestion> completions(String query, String lastToken) throws IOException {
            // Adjust some completions because jline is unable to properly
            // distinguish when a word that contains . or : starts
            var completions = completionEngine.completeQuery(query).stream().map(completion -> {
                var completionType = completion.group();
                if (completionType.equals(SuggestionType.LABEL_OR_RELATIONSHIP.name)) {
                    return new Suggestion(
                            textUntilSentinel(lastToken, ':') + completion.value(),
                            completion.value(),
                            SuggestionType.LABEL_OR_RELATIONSHIP,
                            completion.descr(),
                            completion.complete());
                } else if (completionType.equals(SuggestionType.PROPERTY.name)) {
                    return new Suggestion(
                            textUntilSentinel(lastToken, '.') + completion.value(),
                            completion.value(),
                            SuggestionType.PROPERTY,
                            completion.descr(),
                            completion.complete());
                } else if (completionType.equals(SuggestionType.FUNCTION.name)) {
                    return new Suggestion(
                            textUntilSentinel(lastToken, '.') + completion.value(),
                            completion.value(),
                            SuggestionType.FUNCTION,
                            completion.descr(),
                            completion.complete());
                } else if (completionType.equals(SuggestionType.PROCEDURE.name)) {
                    return new Suggestion(
                            textUntilSentinel(lastToken, '.') + completion.value(),
                            completion.value(),
                            SuggestionType.PROCEDURE,
                            completion.descr(),
                            completion.complete());
                } else {
                    return completion;
                }
            });

            return completions;
        }

        private String queryUntilCompletionWord(CypherCompletion cypher) {
            int cutAt =
                    cypher.cursor() - cypher.wordCursor() - cypher.statement().beginOffset();
            return cypher.statement().statement().substring(0, cutAt);
        }

        @SafeVarargs
        private static <T> Stream<T> concat(Stream<T>... streams) {
            return Stream.of(streams).reduce(Stream::concat).orElseGet(Stream::empty);
        }
    }
}
