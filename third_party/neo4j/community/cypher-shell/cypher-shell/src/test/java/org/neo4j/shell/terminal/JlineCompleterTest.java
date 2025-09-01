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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Condition;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.Parser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.shell.StubDbInfo;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.completions.CompletionEngine;
import org.neo4j.shell.completions.SuggestionType;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parameter.ParameterService.Parameter;
import org.neo4j.shell.parser.ShellStatementParser;

class JlineCompleterTest {
    private record Completion(String completion, String display, String group, String desc) {}

    private ParameterService parameters;
    private JlineCompleter completer;
    private StatementJlineParser parser;
    private StubDbInfo dbInfo;
    private CompletionEngine completionEngine;
    private final LineReader lineReader = mock(LineReader.class);
    private final CommandHelper.CommandFactoryHelper commandHelper = new CommandHelper.CommandFactoryHelper();
    private final List<Completion> allCommands = Stream.of(
                    ":begin",
                    ":commit",
                    ":connect",
                    ":disconnect",
                    ":exit",
                    ":help",
                    ":history",
                    ":param",
                    ":rollback",
                    ":source",
                    ":use",
                    ":impersonate",
                    ":sysinfo",
                    ":access-mode")
            .map(this::command)
            .toList();

    Completion command(String command) {
        var metadata = commandHelper.factoryByName(command).metadata();
        return new Completion(metadata.name(), metadata.name(), SuggestionType.COMMAND.name, metadata.description());
    }

    Completion keyword(String completion) {
        return new Completion(completion, completion, SuggestionType.KEYWORD.name, null);
    }

    Completion identifier(String completion) {
        return new Completion(completion, completion, SuggestionType.IDENTIFIER.name, null);
    }

    Completion parameter(String completion) {
        return new Completion(completion, completion, SuggestionType.PARAMETER.name, null);
    }

    Completion procedureNamespace(String completion) {
        return new Completion(completion, completion, SuggestionType.PROCEDURE.name, "namespace");
    }

    Completion procedureNamespace(String completion, String display) {
        return new Completion(completion, display, SuggestionType.PROCEDURE.name, "namespace");
    }

    Completion procedureCompletion(String completion, String display) {
        return new Completion(completion, display, SuggestionType.PROCEDURE.name, "procedure");
    }

    Completion procedureCompletion(String completion) {
        return new Completion(completion, completion, SuggestionType.PROCEDURE.name, "procedure");
    }

    Completion functionNamespace(String completion, String display) {
        return new Completion(completion, display, SuggestionType.FUNCTION.name, "namespace");
    }

    Completion functionNamespace(String completion) {
        return new Completion(completion, completion, SuggestionType.FUNCTION.name, "namespace");
    }

    Completion functionCompletion(String completion, String display) {
        return new Completion(completion, display, SuggestionType.FUNCTION.name, "function");
    }

    Completion functionCompletion(String completion) {
        return new Completion(completion, completion, SuggestionType.FUNCTION.name, "function");
    }

    Completion labelOrRelType(String completion, String display) {
        return new Completion(completion, display, SuggestionType.LABEL_OR_RELATIONSHIP.name, null);
    }

    Completion labelOrRelType(String completion) {
        return new Completion(completion, completion, SuggestionType.LABEL_OR_RELATIONSHIP.name, null);
    }

    Completion property(String completion, String display) {
        return new Completion(completion, display, SuggestionType.PROPERTY.name, null);
    }

    Completion property(String completion) {
        return new Completion(completion, completion, SuggestionType.PROPERTY.name, null);
    }

    Completion value(String completion) {
        return new Completion(completion, completion, SuggestionType.VALUE.name, null);
    }

    public StubDbInfo dbInfoStub() {
        parameters.setParameters(List.of(new Parameter("intParam", Values.value(1L))));
        parameters.setParameters(List.of(new Parameter("otherIntParam", Values.value(2L))));
        parameters.setParameters(List.of(new Parameter("mapParam", Values.value(Map.of("a", 1)))));
        parameters.setParameters(List.of(new Parameter("stringParam", Values.value("some name"))));

        dbInfo = new StubDbInfo(parameters, true);
        dbInfo.procedures = List.of("foo.bar", "dbms.info", "somethingElse", "foo.info", "db.info");
        dbInfo.functions = List.of("a.b", "xx.yy.fna", "xx.yy.fnb");
        dbInfo.labels = List.of("Actor", "Airport", "Dog", "Gym", "Window", "Wedding");
        dbInfo.relationshipTypes = List.of("ACTED_IN", "DIRECTED", "FOLLOWS", "PRODUCED", "REVIEWED");
        dbInfo.propertyKeys = List.of("born", "data", "id", "name", "nodes", "rating", "relationships");
        dbInfo.databaseNames = List.of("neo4j", "oskar", "system", "Restaurant", "Cafe");
        dbInfo.aliasNames = List.of("alias2", "scoped.alias", "Bar", "Hotel", "Supermarket");
        dbInfo.userNames = List.of("oskar", "neo4j", "admin");
        dbInfo.roleNames = List.of("foo", "bar");

        return dbInfo;
    }

    @BeforeEach
    void setup() {
        var transactionHandler = mock(TransactionHandler.class);
        parameters = ParameterService.create(transactionHandler);
        dbInfo = dbInfoStub();
        completionEngine = new CompletionEngine(dbInfo);
        completer = new JlineCompleter(new CommandHelper.CommandFactoryHelper(), completionEngine);
        parser = new StatementJlineParser(new ShellStatementParser());
        parser.setEnableStatementParsing(true);
    }

    @Test
    void completeBlankSanity() {
        assertThat(complete("")).is(emptyStatementMatcher());
    }

    @Test
    void completeCommandSanity() {
        assertThat(complete("")).containsAll(allCommands);
        assertThat(complete(":")).contains(command(":begin"), command(":commit"), command(":disconnect"));
        assertThat(complete(":he")).contains(command(":help"));
    }

    @Test
    void completeIdentifiersSanity() {
        var whereQuery = "match (myFirstNode:SomeLabel)-[rel]->(mySecondNode) where my";
        var returnQuery = "MATCH (myFirstNode)-[rel]-(mySecondNode) RETURN my";
        var cypher = Stream.of("ALL", "ANY", "COLLECT", "COUNT", "EXISTS")
                .map(this::keyword)
                .toList();
        var identifiers = Stream.of("myFirstNode", "rel", "mySecondNode")
                .map(this::identifier)
                .toList();
        var parameters = Stream.of("$intParam", "$mapParam", "$stringParam", "$otherIntParam")
                .map(this::parameter)
                .toList();

        assertThat(complete(whereQuery))
                .containsAll(cypher)
                .containsAll(identifiers)
                .containsAll(identifiers)
                .containsAll(parameters)
                .doesNotContain(identifier("my"));
        assertThat(complete(returnQuery))
                .containsAll(cypher)
                .containsAll(identifiers)
                .containsAll(identifiers)
                .containsAll(parameters)
                .doesNotContain(identifier("my"));
    }

    @Test
    void completeKeywordsSanity() {
        assertThat(complete("match (n) wh")).contains(keyword("WHERE"));
        assertThat(complete("ma")).contains(keyword("MATCH"));
        assertThat(complete("alter ")).contains(keyword("USER"), keyword("DATABASE"));
        assertThat(complete("RETURN ")).contains(keyword("allShortestPaths"), keyword("shortestPath"));
    }

    @Test
    void completeCypherParametersSanity() {
        assertThat(complete("match (n) where n.p = ")).contains(parameter("$intParam"), parameter("$otherIntParam"));
        assertThat(complete("match (n) where n.p = $intP")).contains(parameter("$intParam"));

        assertThat(complete("match (n) where n.p = $")).contains(parameter("$intParam"), parameter("$otherIntParam"));

        assertThat(complete("ALTER SERVER \"abc\" SET OPTIONS "))
                .contains(parameter("$mapParam"))
                .doesNotContain(parameter("$intParam"), parameter("$otherIntParam"), parameter("$stringParam"));
    }

    @Test
    void completeSecondCypherStatementSanity() {
        assertThat(complete("return 1;")).is(emptyStatementMatcher());
        assertThat(complete("return 1;ret")).contains(keyword("RETURN"));
    }

    @Test
    void completesProcedureNames() {
        assertThat(complete("CALL "))
                .containsExactlyInAnyOrder(
                        procedureNamespace("foo"),
                        procedureNamespace("dbms"),
                        procedureNamespace("db"),
                        procedureCompletion("foo.bar"),
                        procedureCompletion("dbms.info"),
                        procedureCompletion("foo.info"),
                        procedureCompletion("somethingElse"),
                        procedureCompletion("db.info"));

        assertThat(complete("CALL db"))
                .contains(
                        procedureNamespace("dbms"),
                        procedureNamespace("db"),
                        procedureCompletion("dbms.info"),
                        procedureCompletion("db.info"));

        assertThat(complete("CALL db.")).contains(procedureCompletion("db.info", "info"));
    }

    @Test
    void completesFunctionNames() {
        assertThat(complete("RETURN "))
                .contains(
                        functionNamespace("a"),
                        functionNamespace("xx"),
                        functionCompletion("a.b"),
                        functionCompletion("xx.yy.fna"),
                        functionCompletion("xx.yy.fnb"));

        assertThat(complete("RETURN xx")).contains(functionCompletion("xx.yy.fna"), functionCompletion("xx.yy.fnb"));

        assertThat(complete("RETURN xx.")).contains(functionNamespace("xx.yy", "yy"));
    }

    @Test
    void completesLabels() {
        assertThat(complete("MATCH (n: "))
                .contains(labelOrRelType("Actor"), labelOrRelType("Airport"), labelOrRelType("Wedding"));

        assertThat(complete("MATCH (n:"))
                .contains(
                        labelOrRelType("(n:Actor", "Actor"),
                        labelOrRelType("(n:Airport", "Airport"),
                        labelOrRelType("(n:Wedding", "Wedding"));

        assertThat(complete("MATCH (n:Ac")).contains(labelOrRelType("(n:Actor", "Actor"));
    }

    @Test
    void completesRelationshipTypes() {
        assertThat(complete("MATCH (n)-[r:"))
                .contains(
                        labelOrRelType("(n)-[r:ACTED_IN", "ACTED_IN"),
                        labelOrRelType("(n)-[r:DIRECTED", "DIRECTED"),
                        labelOrRelType("(n)-[r:FOLLOWS", "FOLLOWS"));

        assertThat(complete("MATCH (n)-[r: "))
                .contains(labelOrRelType("ACTED_IN"), labelOrRelType("DIRECTED"), labelOrRelType("FOLLOWS"));

        assertThat(complete("MATCH (n)-[r:A")).contains(labelOrRelType("(n)-[r:ACTED_IN", "ACTED_IN"));
    }

    @Test
    void completePropertyKeys() {
        assertThat(complete("RETURN n."))
                .contains(property("n.data", "data"), property("n.born", "born"), property("n.id", "id"));

        assertThat(complete("RETURN n.d")).contains(property("n.data", "data"));
    }

    @Test
    void completeDatabasesAndAliases() {
        assertThat(complete("ALTER DATABASE "))
                .contains(
                        value("neo4j"),
                        value("oskar"),
                        value("system"),
                        value("alias2"),
                        value("scoped.alias"),
                        parameter("$stringParam"))
                .doesNotContain(parameter("$mapParam"));

        assertThat(complete("ALTER DATABASE sco")).contains(value("scoped.alias"));

        assertThat(complete("SHOW ALIAS "))
                .contains(value("scoped.alias"), parameter("$stringParam"))
                .doesNotContain(value("neo4j"), parameter("$mapParam"));
    }

    @Test
    void completeRoleNames() {
        assertThat(complete("GRANT SHOW CONSTRAINT ON DATABASE * TO "))
                .contains(value("foo"), parameter("$stringParam"), value("bar"))
                .doesNotContain(parameter("$mapParam"));
        assertThat(complete("GRANT SHOW CONSTRAINT ON DATABASE * TO f")).contains(value("foo"));
    }

    @Test
    void completeUserNames() {
        assertThat(complete("CREATE USER "))
                .contains(parameter("$stringParam"))
                .doesNotContain(value("oskar"))
                .doesNotContain(value("neo4j"), value("admin"));
        assertThat(complete("SHOW USER "))
                .contains(parameter("$stringParam"), value("oskar"), value("neo4j"), value("admin"))
                .doesNotContain(parameter("$mapParam"));
    }

    @Test
    void completeWithUnicodeSequences() {
        assertThat(complete("M\\u0041TCH (n) ")).contains(keyword("WHERE"), keyword("RETURN"));
    }

    @Test
    void canDisableCompletions() {
        dbInfo.completionsEnabled = false;
        completer = new JlineCompleter(new CommandHelper.CommandFactoryHelper(), completionEngine);
        assertThat(complete("M")).doesNotContain(keyword("MATCH"));
        dbInfo.completionsEnabled = true;
        completer = new JlineCompleter(new CommandHelper.CommandFactoryHelper(), completionEngine);
        assertThat(complete("M")).contains(keyword("MATCH"));
    }

    private List<Completion> complete(String line) {
        var parsed = parser.parse(line, line.length(), Parser.ParseContext.COMPLETE);
        var candidates = new ArrayList<Candidate>();
        completer.complete(lineReader, parsed, candidates);
        var result = candidates.stream()
                .map(c -> new Completion(c.value(), c.displ(), c.group(), c.descr()))
                .toList();
        return result;
    }

    private Condition<List<? extends Completion>> emptyStatementMatcher() {
        var firstKeywords = Stream.of(
                        "CREATE",
                        "MATCH",
                        "DROP",
                        "UNWIND",
                        "RETURN",
                        "WITH",
                        "LOAD CSV",
                        "ALTER",
                        "RENAME",
                        "SHOW",
                        "START DATABASE",
                        "STOP DATABASE")
                .map(this::keyword)
                .toList();
        ;
        var commands = allCommands;
        return new Condition<>(
                items -> items.containsAll(firstKeywords) && items.containsAll(commands), "Empty statement matcher");
    }
}
