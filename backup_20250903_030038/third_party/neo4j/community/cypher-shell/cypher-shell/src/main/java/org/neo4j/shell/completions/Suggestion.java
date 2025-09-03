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
package org.neo4j.shell.completions;

import org.jline.reader.Candidate;
import org.neo4j.shell.commands.Command;

public class Suggestion extends Candidate {
    public Suggestion(String value, SuggestionType type, String desc, boolean complete) {
        super(value, value, type.name, desc, null, null, complete);
    }

    public Suggestion(String value, String display, SuggestionType group, String desc, boolean complete) {
        super(value, display, group.name, desc, null, null, complete);
    }

    public static Suggestion keyword(String completion) {
        return new Suggestion(completion, SuggestionType.KEYWORD, null, false);
    }

    public static Suggestion labelOrRelType(String completion) {
        return new Suggestion(completion, SuggestionType.LABEL_OR_RELATIONSHIP, null, false);
    }

    public static Suggestion property(String completion) {
        return new Suggestion(completion, SuggestionType.PROPERTY, null, false);
    }

    public static Suggestion command(Command.Metadata command) {
        return new Suggestion(command.name(), SuggestionType.COMMAND, command.description(), false);
    }

    public static Suggestion identifier(String identifier) {
        return new Suggestion(identifier, SuggestionType.IDENTIFIER, null, false);
    }

    public static Suggestion parameter(String completion) {
        return new Suggestion(completion, SuggestionType.PARAMETER, null, false);
    }

    public static Suggestion procedure(String completion) {
        return new Suggestion(completion, SuggestionType.PROCEDURE, "procedure", false);
    }

    public static Suggestion procedureNamespace(String completion) {
        return new Suggestion(completion, SuggestionType.PROCEDURE, "namespace", false);
    }

    public static Suggestion function(String completion) {
        return new Suggestion(completion, SuggestionType.FUNCTION, "function", false);
    }

    public static Suggestion functionNamespace(String completion) {
        return new Suggestion(completion, SuggestionType.FUNCTION, "namespace", false);
    }

    public static Suggestion value(String completion) {
        return new Suggestion(completion, SuggestionType.VALUE, null, false);
    }
}
