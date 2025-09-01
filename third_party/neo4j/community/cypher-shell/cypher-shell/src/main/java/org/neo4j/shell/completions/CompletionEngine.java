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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.neo4j.cypher.internal.ast.factory.neo4j.completion.CodeCompletionCore;
import org.neo4j.cypher.internal.parser.v5.Cypher5Lexer;
import org.neo4j.cypher.internal.parser.v5.Cypher5Parser;

public class CompletionEngine {
    public enum ParameterType {
        STRING,
        MAP,
        ANY
    }

    Set<Integer> lexerKeywords;
    Set<Integer> rulesDefiningVariables;
    Set<Integer> rulesDefiningOrUsingVariables;
    Map<Integer, String> customTokenDisplayNames;
    Vocabulary vocabulary;
    DbInfo dbInfo;

    class VariableCollector implements ParseTreeListener {
        private final List<String> variables = new ArrayList<>();
        TokenStream tokens;

        public VariableCollector(TokenStream tokens) {
            this.tokens = tokens;
        }

        @Override
        public void visitTerminal(TerminalNode node) {}

        @Override
        public void visitErrorNode(ErrorNode node) {}

        @Override
        public void enterEveryRule(ParserRuleContext ctx) {}

        @Override
        public void exitEveryRule(ParserRuleContext ctx) {
            if (ctx.getRuleIndex() == Cypher5Parser.RULE_variable) {
                var c = (Cypher5Parser.VariableContext) ctx;
                var variable = c.symbolicVariableNameString().getText();
                // To avoid suggesting the variable that is currently being typed
                // For example RETURN a| <- we don't want to suggest "a" as a variable
                // We check if the variable is in the end of the statement
                var tokenIndex = c.stop.getTokenIndex();
                var nextTokenIsEOF =
                        tokenIndex != -1 && tokens.get(tokenIndex + 1).getType() == Cypher5Lexer.EOF;

                var definesVariable = c.getParent() != null
                        && rulesDefiningOrUsingVariables.contains(c.getParent().getRuleIndex());

                if (variable != null && !nextTokenIsEOF && definesVariable) {
                    this.variables.add(variable);
                }
            }
        }
    }

    public CompletionEngine(DbInfo dbInfo) {
        this.dbInfo = dbInfo;
        this.customTokenDisplayNames = Map.of(
                Cypher5Parser.ALL_SHORTEST_PATHS, "allShortestPaths", Cypher5Parser.SHORTEST_PATH, "shortestPath");
        this.vocabulary = Cypher5Lexer.VOCABULARY;
        var ignoreFromLexer = Set.of(
                Cypher5Lexer.DECIMAL_DOUBLE,
                Cypher5Lexer.UNSIGNED_DECIMAL_INTEGER,
                Cypher5Lexer.UNSIGNED_HEX_INTEGER,
                Cypher5Lexer.UNSIGNED_OCTAL_INTEGER,
                Cypher5Lexer.STRING_LITERAL1,
                Cypher5Lexer.STRING_LITERAL2,
                Cypher5Lexer.ErrorChar,
                Cypher5Lexer.EOF,
                Cypher5Lexer.SPACE,
                Cypher5Lexer.IDENTIFIER,
                Cypher5Lexer.ESCAPED_SYMBOLIC_NAME,
                Cypher5Lexer.MULTI_LINE_COMMENT,
                Cypher5Lexer.SINGLE_LINE_COMMENT);
        this.lexerKeywords = new HashSet<>();
        for (int i = 0; i < Cypher5Lexer.VOCABULARY.getMaxTokenType(); ++i) {
            if (vocabulary.getLiteralName(i) == null && !ignoreFromLexer.contains(i)) {
                this.lexerKeywords.add(i);
            }
        }

        rulesDefiningVariables = Set.of(
                Cypher5Parser.RULE_returnItem,
                Cypher5Parser.RULE_unwindClause,
                Cypher5Parser.RULE_subqueryInTransactionsReportParameters,
                Cypher5Parser.RULE_procedureResultItem,
                Cypher5Parser.RULE_foreachClause,
                Cypher5Parser.RULE_loadCSVClause,
                Cypher5Parser.RULE_reduceExpression,
                Cypher5Parser.RULE_listItemsPredicate,
                Cypher5Parser.RULE_listComprehension);

        rulesDefiningOrUsingVariables = new HashSet(rulesDefiningVariables);
        rulesDefiningOrUsingVariables.addAll(List.of(
                Cypher5Parser.RULE_pattern, Cypher5Parser.RULE_nodePattern, Cypher5Parser.RULE_relationshipPattern));
    }

    public List<Suggestion> completeQuery(String incompleteQuery) throws IOException {
        var lexer = org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstLexer.fromString(incompleteQuery, true);
        var tokenStream = new CommonTokenStream(lexer);
        var parser = new Cypher5Parser(tokenStream);
        var vocabulary = Cypher5Lexer.VOCABULARY;
        var variableCollector = new VariableCollector(tokenStream);
        parser.addParseListener(variableCollector);

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        var rootCtx = parser.statements();
        var stopNode = findStopNode(rootCtx);
        var tokens = tokenStream.getTokens();
        var collectedVariables = variableCollector.variables;
        // The query is always going to have the EOF
        var caretIndex = tokens.size() - 1;
        var previousToken = tokens.size() > 1 ? tokens.get(caretIndex - 1) : null;

        if (previousToken != null
                && (previousToken.getType() == Cypher5Lexer.IDENTIFIER
                        || lexerKeywords.contains(previousToken.getType()))) {
            caretIndex--;
        }

        Set<Integer> preferredRules = Set.of(
                Cypher5Parser.RULE_functionName,
                Cypher5Parser.RULE_procedureName,
                Cypher5Parser.RULE_labelExpression1,
                Cypher5Parser.RULE_symbolicAliasName,
                Cypher5Parser.RULE_parameter,
                Cypher5Parser.RULE_propertyKeyName,
                Cypher5Parser.RULE_variable,
                Cypher5Parser.RULE_leftArrow,
                // this rule is used for usernames and roles.
                Cypher5Parser.RULE_commandNameExpression,
                Cypher5Parser.RULE_symbolicNameString);
        Set<Integer> ignoredTokens = new HashSet<>();

        for (int i = Cypher5Lexer.EOF; i <= vocabulary.getMaxTokenType(); ++i) {
            if (!lexerKeywords.contains(i)) {
                ignoredTokens.add(i);
            }
        }
        var completionEngine = new CodeCompletionCore(parser, preferredRules, ignoredTokens);
        var candidates = completionEngine.collectCandidates(caretIndex, null);
        var tokenCompletions = getTokenCompletions(candidates, ignoredTokens, lexer);
        var ruleCompletions = getRuleCompletions(candidates, collectedVariables, tokens, stopNode);
        var result = new ArrayList<Suggestion>();

        result.addAll(tokenCompletions);
        result.addAll(ruleCompletions);

        return result;
    }

    private Stream<Suggestion> labelCompletions() {
        return this.dbInfo.labels.stream().map(Suggestion::labelOrRelType);
    }

    private Stream<Suggestion> relTypeCompletions() {
        return this.dbInfo.relationshipTypes.stream().map(Suggestion::labelOrRelType);
    }

    private Stream<Suggestion> propertyKeyCompletions() {
        return this.dbInfo.propertyKeys.stream().map(Suggestion::property);
    }

    private ParserRuleContext findStopNode(Cypher5Parser.StatementsContext root) {
        var children = root.children;
        ParserRuleContext current = root;

        while (children != null && !children.isEmpty()) {
            var index = children.size() - 1;
            var child = children.get(index);

            while (index > 0
                    && (child == root.EOF()
                            || child.getText().isEmpty()
                            || child.getText().startsWith("<missing"))) {
                index--;
                child = children.get(index);
            }
            if (child instanceof ParserRuleContext) {
                current = (ParserRuleContext) child;
                children = current.children;
            } else {
                children = null;
            }
        }

        return current;
    }

    private List<Suggestion> getRuleCompletions(
            CodeCompletionCore.CandidatesCollection candidates,
            List<String> collectedVariables,
            List<Token> tokens,
            ParserRuleContext stopNode) {
        return candidates.rules.entrySet().stream()
                .flatMap(entry -> {
                    var ruleNumber = entry.getKey();
                    var candidateRule = entry.getValue();
                    var startTokenIndex = candidateRule.startTokenIndex();
                    var ruleList = candidateRule.ruleList();
                    if (ruleNumber == Cypher5Parser.RULE_functionName) {
                        return functionNameCompletions(startTokenIndex, tokens);
                    } else if (ruleNumber == Cypher5Parser.RULE_procedureName) {
                        return procedureNameCompletions(startTokenIndex, tokens);
                    } else if (ruleNumber == Cypher5Parser.RULE_parameter) {
                        return parameterCompletions(inferExpectedParameterTypeFromContext(candidateRule));
                    } else if (ruleNumber == Cypher5Parser.RULE_propertyKeyName) {
                        var parentRule = ruleList.get(ruleList.size() - 1);
                        var grandParentRule = ruleList.get(ruleList.size() - 2);
                        if (parentRule == Cypher5Parser.RULE_map && grandParentRule == Cypher5Parser.RULE_literal) {
                            return Stream.empty();
                        }

                        var greatGrandParentRule = ruleList.get(ruleList.size() - 3);
                        // When propertyKey is used as postfix to an expr there are many false positives
                        // because expression are very flexible. For this case we only suggest property
                        // keys if the expr is a simple variable that is defined.
                        // We still don't know the type of the variable we're completing without a symbol table
                        // but it is likely to be a node/relationship
                        if (parentRule == Cypher5Parser.RULE_property
                                && grandParentRule == Cypher5Parser.RULE_postFix
                                && greatGrandParentRule == Cypher5Parser.RULE_expression2) {
                            var expr2 = stopNode.getParent().getParent().getParent();
                            if (expr2 instanceof Cypher5Parser.Expression2Context) {
                                var variableName = ((Cypher5Parser.Expression2Context) expr2)
                                        .expression1()
                                        .variable()
                                        .getText();
                                if (variableName == null || collectedVariables.contains(variableName)) {
                                    return Stream.empty();
                                }
                            }
                        }

                        return propertyKeyCompletions();
                    } else if (ruleNumber == Cypher5Parser.RULE_variable) {
                        if (!ruleList.isEmpty()) {
                            var parentRule = ruleList.get(ruleList.size() - 1);

                            if (!rulesDefiningVariables.contains(parentRule)) {
                                return collectedVariables.stream().map(Suggestion::identifier);
                            }
                        }
                    } else if (ruleNumber == Cypher5Parser.RULE_labelExpression1) {
                        var topExprIndex = ruleList.indexOf(Cypher5Parser.RULE_labelExpression);

                        if (topExprIndex > 0) {
                            var topExprParent = ruleList.get(topExprIndex - 1);
                            if (topExprParent == Cypher5Parser.RULE_nodePattern) {
                                return labelCompletions();
                            }

                            if (topExprParent == Cypher5Parser.RULE_relationshipPattern) {
                                return relTypeCompletions();
                            }

                            return Stream.concat(labelCompletions(), relTypeCompletions());
                        }
                    } else if (ruleNumber == Cypher5Parser.RULE_symbolicAliasName) {
                        return completeAliasName(tokens, candidateRule, startTokenIndex);
                    } else if (ruleNumber == Cypher5Parser.RULE_commandNameExpression) {
                        return completeSymbolicName(candidateRule, tokens, startTokenIndex);
                    }

                    return Stream.empty();
                })
                .collect(Collectors.toList());
    }

    private CompletionEngine.ParameterType inferExpectedParameterTypeFromContext(
            CodeCompletionCore.CandidateRule candidateRule) {
        var ruleList = candidateRule.ruleList();
        var parentRule = ruleList.get(ruleList.size() - 1);

        if (Set.of(
                        Cypher5Parser.RULE_stringOrParameter,
                        Cypher5Parser.RULE_commandNameExpression,
                        Cypher5Parser.RULE_symbolicNameOrStringParameter,
                        Cypher5Parser.RULE_symbolicNameOrStringParameterList,
                        Cypher5Parser.RULE_symbolicAliasNameOrParameter,
                        Cypher5Parser.RULE_passwordExpression,
                        Cypher5Parser.RULE_createUser,
                        Cypher5Parser.RULE_dropUser,
                        Cypher5Parser.RULE_alterUser,
                        Cypher5Parser.RULE_renameUser,
                        Cypher5Parser.RULE_createRole,
                        Cypher5Parser.RULE_dropRole,
                        Cypher5Parser.RULE_userNames,
                        Cypher5Parser.RULE_roleNames,
                        Cypher5Parser.RULE_renameRole)
                .contains(parentRule)) {
            return CompletionEngine.ParameterType.STRING;
        } else if (Set.of(Cypher5Parser.RULE_properties, Cypher5Parser.RULE_mapOrParameter)
                .contains(parentRule)) {
            return CompletionEngine.ParameterType.MAP;
        } else {
            return CompletionEngine.ParameterType.ANY;
        }
    }

    private Optional<Token> findPreviousNonSpace(List<Token> tokens, int index) {
        var i = index;
        while (i > 0) {
            var token = tokens.get(--i);

            if (token.getType() != Cypher5Parser.SPACE) {
                return Optional.of(token);
            }
        }

        return Optional.empty();
    }

    private Stream<Suggestion> completeSymbolicName(
            CodeCompletionCore.CandidateRule candidateRule, List<Token> tokens, int ruleStartTokenIndex) {
        // parameters are valid values in all cases of symbolic name
        var parameterSuggestions = parameterCompletions(inferExpectedParameterTypeFromContext(candidateRule));
        var ruleList = candidateRule.ruleList();

        var rulesCreatingNewUserOrRole = List.of(Cypher5Parser.RULE_createUser, Cypher5Parser.RULE_createRole);

        var previousToken = findPreviousNonSpace(tokens, ruleStartTokenIndex);
        var afterToToken = previousToken.stream().anyMatch(t -> t.getType() == Cypher5Parser.TO);

        // avoid suggesting existing user names or role names when creating a new one
        if (rulesCreatingNewUserOrRole.stream().anyMatch(ruleList::contains)
                ||
                // We are suggesting an user as target for the renaming
                //      RENAME USER existing TO target
                // so target should be non-existent
                (candidateRule.ruleList().contains(Cypher5Parser.RULE_renameUser) && afterToToken)) {
            return parameterSuggestions;
        }

        var rulesThatAcceptExistingUsers = List.of(
                Cypher5Parser.RULE_dropUser,
                Cypher5Parser.RULE_renameUser,
                Cypher5Parser.RULE_alterUser,
                Cypher5Parser.RULE_userNames);

        if (rulesThatAcceptExistingUsers.stream().anyMatch(ruleList::contains)) {
            return Stream.concat(parameterSuggestions, dbInfo.userNames.stream().map(Suggestion::value));
        }

        var rulesThatAcceptExistingRoles =
                List.of(Cypher5Parser.RULE_roleNames, Cypher5Parser.RULE_dropRole, Cypher5Parser.RULE_renameRole);

        if (rulesThatAcceptExistingRoles.stream().anyMatch(ruleList::contains)) {
            return Stream.concat(parameterSuggestions, dbInfo.roleNames.stream().map(Suggestion::value));
        }

        return Stream.empty();
    }

    private Stream<Suggestion> completeAliasName(
            List<Token> tokens, CodeCompletionCore.CandidateRule candidateRule, int ruleStartTokenIndex) {
        var ruleList = candidateRule.ruleList();
        // The rule for RULE_symbolicAliasName technically allows for spaces given that a dot is included in the name
        // so ALTER ALIAS a . b  FOR DATABASE neo4j is accepted by neo4j. It does however only drop the spaces for the
        // alias
        // it becomes just a.b

        // The issue for us is that when we complete "ALTER ALIAS a " <- according to the grammar points say we could
        // still be building a name
        // To handle this we check if the token after the first identifier in the rule is a space (as opposed to a dot)
        // if so we have a false positive and we return null to ignore the rule
        // symbolicAliasName: (symbolicNameString (DOT symbolicNameString)* | parameter);
        if (ruleStartTokenIndex + 1 < tokens.size()
                && tokens.get(ruleStartTokenIndex + 1).getType() == Cypher5Lexer.SPACE) {
            return Stream.empty();
        }

        // parameters are valid values in all cases of symbolicAliasName
        var parameterSuggestions = parameterCompletions(CompletionEngine.ParameterType.STRING);
        var rulesCreatingNewDb = List.of(Cypher5Parser.RULE_createDatabase, Cypher5Parser.RULE_createCompositeDatabase);

        // avoid suggesting existing database names when creating a new database
        if (rulesCreatingNewDb.stream().anyMatch(ruleList::contains)) {
            return parameterSuggestions;
        }

        // For `CREATE ALIAS aliasName FOR DATABASE databaseName`
        // Should not suggest existing aliases for aliasName but should suggest existing databases for databaseName
        // so we return base suggestions if we're at the `aliasName` rule
        if (ruleList.contains(Cypher5Parser.RULE_createAlias) && ruleList.contains(Cypher5Parser.RULE_aliasName)) {
            return parameterSuggestions;
        }

        var rulesThatOnlyAcceptAlias =
                List.of(Cypher5Parser.RULE_dropAlias, Cypher5Parser.RULE_alterAlias, Cypher5Parser.RULE_showAliases);

        if (rulesThatOnlyAcceptAlias.stream().anyMatch(ruleList::contains)) {
            return Stream.concat(
                    parameterSuggestions, dbInfo.aliasNames.stream().map(Suggestion::value));
        }

        return Stream.concat(
                Stream.concat(
                        parameterSuggestions, dbInfo.databaseNames.stream().map(Suggestion::value)),
                dbInfo.aliasNames.stream().map(Suggestion::value));
    }

    private String calculateNamespacePrefix(int startTokenIndex, List<Token> tokens) {
        var ruleTokens = tokens.subList(startTokenIndex, tokens.size() - 1);
        var lastNonEOFToken = ruleTokens.size() >= 2 ? ruleTokens.get(ruleTokens.size() - 2) : null;

        var nonSpaceTokens = new ArrayList<>(ruleTokens.stream()
                .filter((token) -> token.getType() != Cypher5Lexer.SPACE && token.getType() != Cypher5Lexer.EOF)
                .toList());

        var lastNonSpaceIsDot = !nonSpaceTokens.isEmpty()
                && nonSpaceTokens.get(nonSpaceTokens.size() - 1).getType() == Cypher5Lexer.DOT;

        // `gds version` is invalid but `gds .version` and `gds. version` are valid
        // so if the last token is a space and the last non-space token
        // is anything but a dot return empty completions to avoid
        // creating invalid suggestions (db ping)
        if (lastNonEOFToken != null && lastNonEOFToken.getType() == Cypher5Lexer.SPACE && !lastNonSpaceIsDot) {
            return null;
        }

        // calculate the current namespace prefix
        // only keep finished namespaces both second level `gds.ver` => `gds.`
        // and first level make `gds` => ''
        if (!lastNonSpaceIsDot && !nonSpaceTokens.isEmpty()) {
            nonSpaceTokens.remove(nonSpaceTokens.size() - 1);
        }

        var namespacePrefix = nonSpaceTokens.stream().map(Token::getText).collect(Collectors.joining(""));
        return namespacePrefix;
    }

    private Stream<Suggestion> functionNameCompletions(int ruleStartTokenIndex, List<Token> tokens) {
        return namespacedCompletion(ruleStartTokenIndex, tokens, dbInfo.functions, SuggestionType.FUNCTION);
    }

    private Stream<Suggestion> procedureNameCompletions(int ruleStartTokenIndex, List<Token> tokens) {
        return namespacedCompletion(ruleStartTokenIndex, tokens, dbInfo.procedures, SuggestionType.PROCEDURE);
    }

    private Stream<Suggestion> getNamespaceSuggestions(Stream<String> namespaces, SuggestionType suggestionType) {
        return namespaces
                .map((completion) -> {
                    if (suggestionType == SuggestionType.FUNCTION) {
                        return Suggestion.functionNamespace(completion);
                    } else {
                        return Suggestion.procedureNamespace(completion);
                    }
                })
                .collect(Collectors.toSet())
                .stream();
    }

    private Stream<Suggestion> getFullNameSuggestions(Stream<String> fullNames, SuggestionType suggestionType) {
        return fullNames.map((completion) -> {
            if (suggestionType == SuggestionType.FUNCTION) {
                return Suggestion.function(completion);
            } else {
                return Suggestion.procedure(completion);
            }
        });
    }

    private Stream<Suggestion> namespacedCompletion(
            int ruleStartTokenIndex, List<Token> tokens, List<String> signatures, SuggestionType suggestionType) {
        var fullNames = new HashSet<>(signatures);
        var namespacePrefix = calculateNamespacePrefix(ruleStartTokenIndex, tokens);
        if (namespacePrefix == null) {
            return Stream.empty();
        }

        if (namespacePrefix.isEmpty()) {
            // If we don't have any prefix show full functions and top level namespaces
            var topLevelPrefixes =
                    fullNames.stream().filter((fn) -> fn.contains(".")).map((fnName) -> fnName.split("\\.")[0]);
            var namespaceCompletions = getNamespaceSuggestions(topLevelPrefixes, suggestionType);
            var fullNameCompletions = getFullNameSuggestions(fullNames.stream(), suggestionType);
            return Stream.concat(namespaceCompletions, fullNameCompletions);
        } else {
            // if we have a namespace prefix, complete on the namespace level:
            // apoc. => show `util` | `load` | `date` etc.
            var fullNameOptions = new HashSet<String>();
            var namespaceOptions = new HashSet<String>();

            for (String name : fullNames) {
                if (name.startsWith(namespacePrefix)) {
                    // given prefix `apoc.` turn `apoc.util.sleep` => `util`
                    var splitByDot = name.substring(namespacePrefix.length()).split("\\.");
                    var option = splitByDot[0];
                    var isFunctionName = splitByDot.length == 1;

                    // handle prefix `time.truncate.` turning `time.truncate` => ``
                    if (!option.isEmpty()) {
                        if (isFunctionName) {
                            fullNameOptions.add(option);
                        } else {
                            namespaceOptions.add(option);
                        }
                    }
                }
            }
            var namespaceCompletions = getNamespaceSuggestions(namespaceOptions.stream(), suggestionType);
            var fullNameCompletions = getFullNameSuggestions(fullNameOptions.stream(), suggestionType);
            return Stream.concat(namespaceCompletions, fullNameCompletions);
        }
    }
    ;

    private Stream<Suggestion> parameterCompletions(CompletionEngine.ParameterType expectedType) {
        var result = this.dbInfo.parameters().entrySet().stream()
                .filter(entry -> expectedType == CompletionEngine.ParameterType.ANY || entry.getValue() == expectedType)
                .map((parameter) -> Suggestion.parameter("$" + parameter.getKey()));
        return result;
    }

    private String getTokenName(int token) {
        if (this.customTokenDisplayNames.containsKey(token)) {
            return this.customTokenDisplayNames.get(token);
        } else {
            return vocabulary.getDisplayName(token);
        }
    }

    private List<Suggestion> getTokenCompletions(
            CodeCompletionCore.CandidatesCollection candidates, Set<Integer> ignoredTokens, Cypher5Lexer cypherLexer) {
        var tokenEntries = candidates.tokens.entrySet();
        Stream<String> completions = tokenEntries.stream().flatMap((value) -> {
            var tokenNumber = value.getKey();
            var followUpList = value.getValue();
            if (!ignoredTokens.contains(tokenNumber)) {
                var firstToken = getTokenName(tokenNumber);
                var lastIndexToSlice = followUpList.size();

                for (int i = 0; i < followUpList.size() && lastIndexToSlice == followUpList.size(); ++i) {
                    if (ignoredTokens.contains(followUpList.get(i))) {
                        lastIndexToSlice = i;
                    }
                }

                var followUpTokens = followUpList.subList(0, lastIndexToSlice);
                var followUpString =
                        followUpTokens.stream().map(this::getTokenName).collect(Collectors.joining("  "));

                if (!followUpString.isEmpty()) {
                    return Stream.of(firstToken + " " + followUpString);
                } else {
                    return Stream.of(firstToken);
                }
            }

            return Stream.of();
        });
        var result = completions.map(Suggestion::keyword).collect(Collectors.toList());

        return result;
    }

    public boolean completionsEnabled() {
        return dbInfo.completionsEnabled();
    }
}
