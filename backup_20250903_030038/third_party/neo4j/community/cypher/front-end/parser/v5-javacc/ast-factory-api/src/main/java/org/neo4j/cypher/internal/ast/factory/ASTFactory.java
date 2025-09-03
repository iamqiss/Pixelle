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
package org.neo4j.cypher.internal.ast.factory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.cypher.internal.parser.common.ast.factory.AccessType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ActionType;
import org.neo4j.cypher.internal.parser.common.ast.factory.CallInTxsOnErrorBehaviourType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ConstraintType;
import org.neo4j.cypher.internal.parser.common.ast.factory.CreateIndexTypes;
import org.neo4j.cypher.internal.parser.common.ast.factory.HintIndexType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ParserCypherTypeName;
import org.neo4j.cypher.internal.parser.common.ast.factory.ScopeType;
import org.neo4j.cypher.internal.parser.common.ast.factory.ShowCommandFilterTypes;
import org.neo4j.cypher.internal.parser.common.ast.factory.SimpleEither;

/**
 * Factory for constructing ASTs.
 * <p>
 * This interface is generic in many dimensions, in order to support type-safe construction of ASTs
 * without depending on the concrete AST type. This architecture allows code which creates/manipulates AST
 * to live independently of the AST, and thus makes sharing and reuse of these components much easier.
 * <p>
 * The factory contains methods for creating AST representing all of Cypher 9, as defined
 * at `https://github.com/opencypher/openCypher/`, and implemented in `https://github.com/opencypher/front-end`.
 * <p>
 * Schema commands like `CREATE/DROP INDEX` as not supported, nor system DSL used in Neo4j.
 *
 * @param <POS> type used to mark the input position of the created AST node.
 */
public interface ASTFactory<
                STATEMENTS,
                STATEMENT,
                QUERY extends STATEMENT,
                CLAUSE,
                FINISH_CLAUSE extends CLAUSE,
                RETURN_CLAUSE extends CLAUSE,
                RETURN_ITEM,
                RETURN_ITEMS,
                ORDER_ITEM,
                PATTERN,
                NODE_PATTERN extends PATTERN_ATOM,
                REL_PATTERN extends PATTERN_ATOM,
                PATH_LENGTH,
                SET_CLAUSE extends CLAUSE,
                SET_ITEM,
                REMOVE_ITEM,
                CALL_RESULT_ITEM,
                HINT,
                EXPRESSION,
                LABEL_EXPRESSION,
                FUNCTION_INVOCATION extends EXPRESSION,
                PARAMETER extends EXPRESSION,
                VARIABLE extends EXPRESSION,
                PROPERTY extends EXPRESSION,
                MAP_PROJECTION_ITEM,
                USE_GRAPH extends CLAUSE,
                STATEMENT_WITH_GRAPH extends STATEMENT,
                ADMINISTRATION_COMMAND extends STATEMENT_WITH_GRAPH,
                SCHEMA_COMMAND extends STATEMENT_WITH_GRAPH,
                YIELD extends CLAUSE,
                WHERE,
                DATABASE_SCOPE,
                WAIT_CLAUSE,
                ADMINISTRATION_ACTION,
                GRAPH_SCOPE,
                PRIVILEGE_TYPE,
                PRIVILEGE_RESOURCE,
                PRIVILEGE_QUALIFIER,
                AUTH,
                AUTH_ATTRIBUTE,
                SUBQUERY_IN_TRANSACTIONS_PARAMETERS,
                SUBQUERY_IN_TRANSACTIONS_BATCH_PARAMETERS,
                SUBQUERY_IN_TRANSACTIONS_CONCURRENCY_PARAMETERS,
                SUBQUERY_IN_TRANSACTIONS_ERROR_PARAMETERS,
                SUBQUERY_IN_TRANSACTIONS_REPORT_PARAMETERS,
                POS,
                ENTITY_TYPE,
                PATH_PATTERN_QUANTIFIER,
                PATTERN_ATOM,
                DATABASE_NAME,
                PATTERN_SELECTOR,
                MATCH_MODE,
                PATTERN_ELEMENT>
        extends ASTExpressionFactory<
                EXPRESSION,
                LABEL_EXPRESSION,
                PARAMETER,
                PATTERN,
                QUERY,
                WHERE,
                VARIABLE,
                PROPERTY,
                FUNCTION_INVOCATION,
                MAP_PROJECTION_ITEM,
                POS,
                ENTITY_TYPE,
                MATCH_MODE> {
    final class NULL {
        private NULL() {
            throw new IllegalStateException("This class should not be instantiated, use `null` instead.");
        }
    }

    class StringPos<POS> {
        public final String string;
        public final POS pos;
        public final POS endPos;

        public StringPos(String string, POS pos) {
            this.string = string;
            this.pos = pos;
            this.endPos = null;
        }

        public StringPos(String string, POS pos, POS endPos) {
            this.string = string;
            this.pos = pos;
            this.endPos = endPos;
        }
    }

    STATEMENTS statements(List<STATEMENT> statements);

    // QUERY

    QUERY newSingleQuery(POS p, List<CLAUSE> clauses);

    QUERY newSingleQuery(List<CLAUSE> clauses);

    QUERY newUnion(POS p, QUERY lhs, QUERY rhs, boolean all);

    USE_GRAPH directUseClause(POS p, DATABASE_NAME name);

    USE_GRAPH functionUseClause(POS p, FUNCTION_INVOCATION function);

    FINISH_CLAUSE newFinishClause(POS p);

    RETURN_CLAUSE newReturnClause(
            POS p,
            boolean distinct,
            RETURN_ITEMS returnItems,
            List<ORDER_ITEM> order,
            POS orderPos,
            EXPRESSION skip,
            POS skipPosition,
            EXPRESSION limit,
            POS limitPosition);

    RETURN_ITEMS newReturnItems(POS p, boolean returnAll, List<RETURN_ITEM> returnItems);

    RETURN_ITEM newReturnItem(POS p, EXPRESSION e, VARIABLE v);

    RETURN_ITEM newReturnItem(POS p, EXPRESSION e, int eStartOffset, int eEndOffset);

    ORDER_ITEM orderDesc(POS p, EXPRESSION e);

    ORDER_ITEM orderAsc(POS p, EXPRESSION e);

    WHERE whereClause(POS p, EXPRESSION e);

    CLAUSE withClause(POS p, RETURN_CLAUSE returnClause, WHERE where);

    CLAUSE matchClause(
            POS p,
            boolean optional,
            MATCH_MODE matchMode,
            List<PATTERN> patterns,
            POS patternPos,
            List<HINT> hints,
            WHERE where);

    HINT usingIndexHint(
            POS p,
            VARIABLE v,
            String labelOrRelType,
            List<String> properties,
            boolean seekOnly,
            HintIndexType indexType);

    HINT usingJoin(POS p, List<VARIABLE> joinVariables);

    HINT usingScan(POS p, VARIABLE v, String labelOrRelType);

    CLAUSE createClause(POS p, List<PATTERN> patterns);

    CLAUSE insertClause(POS p, List<PATTERN> patterns);

    SET_CLAUSE setClause(POS p, List<SET_ITEM> setItems);

    SET_ITEM setProperty(PROPERTY property, EXPRESSION value);

    SET_ITEM setDynamicProperty(EXPRESSION dynamicProperty, EXPRESSION value);

    SET_ITEM setVariable(VARIABLE variable, EXPRESSION value);

    SET_ITEM addAndSetVariable(VARIABLE variable, EXPRESSION value);

    SET_ITEM setLabels(
            VARIABLE variable, List<StringPos<POS>> labels, List<EXPRESSION> dynamicLabels, boolean containsIs);

    CLAUSE removeClause(POS p, List<REMOVE_ITEM> removeItems);

    REMOVE_ITEM removeProperty(PROPERTY property);

    REMOVE_ITEM removeDynamicProperty(EXPRESSION dynamicProperty);

    REMOVE_ITEM removeLabels(
            VARIABLE variable, List<StringPos<POS>> labels, List<EXPRESSION> dynamicLabels, boolean containsIs);

    CLAUSE deleteClause(POS p, boolean detach, List<EXPRESSION> expressions);

    CLAUSE unwindClause(POS p, EXPRESSION e, VARIABLE v);

    enum MergeActionType {
        OnCreate,
        OnMatch
    }

    CLAUSE mergeClause(
            POS p,
            PATTERN pattern,
            List<SET_CLAUSE> setClauses,
            List<MergeActionType> actionTypes,
            List<POS> positions);

    CLAUSE callClause(
            POS p,
            POS namespacePosition,
            POS procedureNamePosition,
            POS procedureResultPosition,
            List<String> namespace,
            String name,
            List<EXPRESSION> arguments,
            boolean yieldAll,
            List<CALL_RESULT_ITEM> resultItems,
            WHERE where,
            boolean optional);

    CALL_RESULT_ITEM callResultItem(POS p, String name, VARIABLE v);

    PATTERN patternWithSelector(PATTERN_SELECTOR selector, PATTERN patternPart);

    PATTERN namedPattern(VARIABLE v, PATTERN pattern);

    PATTERN shortestPathPattern(POS p, PATTERN_ELEMENT patternElement);

    PATTERN allShortestPathsPattern(POS p, PATTERN_ELEMENT patternElement);

    PATTERN pathPattern(PATTERN_ELEMENT patternElement);

    PATTERN insertPathPattern(List<PATTERN_ATOM> atoms);

    PATTERN_ELEMENT patternElement(List<PATTERN_ATOM> atoms);

    PATTERN_SELECTOR anyPathSelector(String count, POS countPosition, POS position);

    PATTERN_SELECTOR allPathSelector(POS position);

    PATTERN_SELECTOR anyShortestPathSelector(String count, POS countPosition, POS position);

    PATTERN_SELECTOR allShortestPathSelector(POS position);

    PATTERN_SELECTOR shortestGroupsSelector(String count, POS countPosition, POS position);

    NODE_PATTERN nodePattern(
            POS p, VARIABLE v, LABEL_EXPRESSION labelExpression, EXPRESSION properties, EXPRESSION predicate);

    REL_PATTERN relationshipPattern(
            POS p,
            boolean left,
            boolean right,
            VARIABLE v,
            LABEL_EXPRESSION labelExpression,
            PATH_LENGTH pathLength,
            EXPRESSION properties,
            EXPRESSION predicate);

    /**
     * Create a path-length object used to specify path lengths for variable length patterns.
     *
     * Note that paths will be reported in a quite specific manner:
     * Cypher       minLength   maxLength
     * ----------------------------------
     * [*]          null        null
     * [*2]         "2"         "2"
     * [*2..]       "2"         ""
     * [*..3]       ""          "3"
     * [*2..3]      "2"         "3"
     * [*..]        ""          ""      <- separate from [*] to allow specific error messages
     */
    PATH_LENGTH pathLength(POS p, POS pMin, POS pMax, String minLength, String maxLength);

    PATH_PATTERN_QUANTIFIER intervalPathQuantifier(
            POS p, POS posLowerBound, POS posUpperBound, String lowerBound, String upperBound);

    PATH_PATTERN_QUANTIFIER fixedPathQuantifier(POS p, POS valuePos, String value);

    PATH_PATTERN_QUANTIFIER plusPathQuantifier(POS p);

    PATH_PATTERN_QUANTIFIER starPathQuantifier(POS p);

    MATCH_MODE repeatableElements(POS p);

    MATCH_MODE differentRelationships(POS p);

    PATTERN_ATOM parenthesizedPathPattern(
            POS p, PATTERN internalPattern, EXPRESSION where, PATH_PATTERN_QUANTIFIER quantifier);

    PATTERN_ATOM quantifiedRelationship(REL_PATTERN rel, PATH_PATTERN_QUANTIFIER quantifier);

    CLAUSE loadCsvClause(POS p, boolean headers, EXPRESSION source, VARIABLE v, String fieldTerminator);

    CLAUSE foreachClause(POS p, VARIABLE v, EXPRESSION list, List<CLAUSE> clauses);

    CLAUSE subqueryClause(
            POS p,
            QUERY subquery,
            SUBQUERY_IN_TRANSACTIONS_PARAMETERS inTransactions,
            boolean scopeAll,
            boolean hasScope,
            List<VARIABLE> variables,
            boolean optional);

    SUBQUERY_IN_TRANSACTIONS_PARAMETERS subqueryInTransactionsParams(
            POS p,
            SUBQUERY_IN_TRANSACTIONS_BATCH_PARAMETERS batchParams,
            SUBQUERY_IN_TRANSACTIONS_CONCURRENCY_PARAMETERS concurrencyParams,
            SUBQUERY_IN_TRANSACTIONS_ERROR_PARAMETERS errorParams,
            SUBQUERY_IN_TRANSACTIONS_REPORT_PARAMETERS reportParams);

    SUBQUERY_IN_TRANSACTIONS_BATCH_PARAMETERS subqueryInTransactionsBatchParameters(POS p, EXPRESSION batchSize);

    SUBQUERY_IN_TRANSACTIONS_CONCURRENCY_PARAMETERS subqueryInTransactionsConcurrencyParameters(
            POS p, EXPRESSION concurrency);

    SUBQUERY_IN_TRANSACTIONS_ERROR_PARAMETERS subqueryInTransactionsErrorParameters(
            POS p, CallInTxsOnErrorBehaviourType onErrorBehaviour);

    SUBQUERY_IN_TRANSACTIONS_REPORT_PARAMETERS subqueryInTransactionsReportParameters(POS p, VARIABLE v);

    CLAUSE orderBySkipLimitClause(
            POS t, List<ORDER_ITEM> order, POS orderPos, EXPRESSION skip, POS skipPos, EXPRESSION limit, POS limitPos);
    // Commands
    STATEMENT_WITH_GRAPH useGraph(STATEMENT_WITH_GRAPH statement, USE_GRAPH useGraph);

    // Show Command Clauses

    YIELD yieldClause(
            POS p,
            boolean returnAll,
            List<RETURN_ITEM> returnItems,
            POS returnItemsPosition,
            List<ORDER_ITEM> orderBy,
            POS orderPos,
            EXPRESSION skip,
            POS skipPosition,
            EXPRESSION limit,
            POS limitPosition,
            WHERE where);

    CLAUSE showIndexClause(POS p, ShowCommandFilterTypes indexType, WHERE where, YIELD yieldClause);

    CLAUSE showConstraintClause(POS p, ShowCommandFilterTypes constraintType, WHERE where, YIELD yieldClause);

    CLAUSE showProcedureClause(POS p, boolean currentUser, String user, WHERE where, YIELD yieldClause);

    CLAUSE showFunctionClause(
            POS p,
            ShowCommandFilterTypes functionType,
            boolean currentUser,
            String user,
            WHERE where,
            YIELD yieldClause);

    CLAUSE showTransactionsClause(POS p, SimpleEither<List<String>, EXPRESSION> ids, WHERE where, YIELD yieldClause);

    CLAUSE terminateTransactionsClause(
            POS p, SimpleEither<List<String>, EXPRESSION> ids, WHERE where, YIELD yieldClause);

    CLAUSE showSettingsClause(POS p, SimpleEither<List<String>, EXPRESSION> names, WHERE where, YIELD yieldClause);

    CLAUSE turnYieldToWith(YIELD yieldClause);

    // Schema Commands
    // Constraint Commands

    SCHEMA_COMMAND createConstraint(
            POS p,
            ConstraintType constraintType,
            boolean replace,
            boolean ifNotExists,
            SimpleEither<StringPos<POS>, PARAMETER> constraintName,
            VARIABLE variable,
            StringPos<POS> label,
            List<PROPERTY> properties,
            ParserCypherTypeName propertyType,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> options);

    SCHEMA_COMMAND dropConstraint(POS p, SimpleEither<StringPos<POS>, PARAMETER> name, boolean ifExists);

    // Index Commands

    SCHEMA_COMMAND createLookupIndex(
            POS p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<POS>, PARAMETER> indexName,
            VARIABLE variable,
            StringPos<POS> functionName,
            VARIABLE functionParameter,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> options);

    SCHEMA_COMMAND createIndex(
            POS p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<POS>, PARAMETER> indexName,
            VARIABLE variable,
            StringPos<POS> label,
            List<PROPERTY> properties,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> options,
            CreateIndexTypes indexType);

    SCHEMA_COMMAND createFulltextIndex(
            POS p,
            boolean replace,
            boolean ifNotExists,
            boolean isNode,
            SimpleEither<StringPos<POS>, PARAMETER> indexName,
            VARIABLE variable,
            List<StringPos<POS>> labels,
            List<PROPERTY> properties,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> options);

    SCHEMA_COMMAND dropIndex(POS p, SimpleEither<StringPos<POS>, PARAMETER> name, boolean ifExists);

    // Administration Commands
    // Role Administration Commands

    ADMINISTRATION_COMMAND createRole(
            POS p,
            boolean replace,
            SimpleEither<StringPos<POS>, PARAMETER> roleName,
            SimpleEither<StringPos<POS>, PARAMETER> fromRole,
            boolean ifNotExists,
            boolean immutable);

    ADMINISTRATION_COMMAND dropRole(POS p, SimpleEither<StringPos<POS>, PARAMETER> roleName, boolean ifExists);

    ADMINISTRATION_COMMAND renameRole(
            POS p,
            SimpleEither<StringPos<POS>, PARAMETER> fromRoleName,
            SimpleEither<StringPos<POS>, PARAMETER> toRoleName,
            boolean ifExists);

    ADMINISTRATION_COMMAND showRoles(
            POS p, boolean withUsers, boolean showAll, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where);

    ADMINISTRATION_COMMAND grantRoles(
            POS p,
            List<SimpleEither<StringPos<POS>, PARAMETER>> roles,
            List<SimpleEither<StringPos<POS>, PARAMETER>> users);

    ADMINISTRATION_COMMAND revokeRoles(
            POS p,
            List<SimpleEither<StringPos<POS>, PARAMETER>> roles,
            List<SimpleEither<StringPos<POS>, PARAMETER>> users);

    // User Administration Commands

    ADMINISTRATION_COMMAND createUser(
            POS p,
            boolean replace,
            boolean ifNotExists,
            SimpleEither<StringPos<POS>, PARAMETER> username,
            Boolean suspended,
            DATABASE_NAME homeDatabase,
            List<AUTH> auths,
            List<AUTH_ATTRIBUTE> systemAuthAttributes);

    ADMINISTRATION_COMMAND dropUser(POS p, boolean ifExists, SimpleEither<StringPos<POS>, PARAMETER> username);

    ADMINISTRATION_COMMAND renameUser(
            POS p,
            SimpleEither<StringPos<POS>, PARAMETER> fromUserName,
            SimpleEither<StringPos<POS>, PARAMETER> toUserName,
            boolean ifExists);

    ADMINISTRATION_COMMAND setOwnPassword(POS p, EXPRESSION currentPassword, EXPRESSION newPassword);

    ADMINISTRATION_COMMAND alterUser(
            POS p,
            boolean ifExists,
            SimpleEither<StringPos<POS>, PARAMETER> username,
            Boolean suspended,
            DATABASE_NAME homeDatabase,
            boolean removeHome,
            List<AUTH> auths,
            List<AUTH_ATTRIBUTE> systemAuthAttributes,
            boolean removeAllAuth,
            List<EXPRESSION> removeAuths);

    AUTH auth(String provider, List<AUTH_ATTRIBUTE> attributes, POS p);

    AUTH_ATTRIBUTE authId(POS s, EXPRESSION id);

    AUTH_ATTRIBUTE password(POS p, EXPRESSION password, boolean encrypted);

    AUTH_ATTRIBUTE passwordChangeRequired(POS p, boolean changeRequired);

    EXPRESSION passwordExpression(PARAMETER password);

    EXPRESSION passwordExpression(POS s, POS e, String password);

    ADMINISTRATION_COMMAND showUsers(
            POS p, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where, boolean withAuth);

    ADMINISTRATION_COMMAND showCurrentUser(POS p, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where);

    // Privilege Commands

    ADMINISTRATION_COMMAND showSupportedPrivileges(
            POS p, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where);

    ADMINISTRATION_COMMAND showAllPrivileges(
            POS p, boolean asCommand, boolean asRevoke, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where);

    ADMINISTRATION_COMMAND showRolePrivileges(
            POS p,
            List<SimpleEither<StringPos<POS>, PARAMETER>> roles,
            boolean asCommand,
            boolean asRevoke,
            YIELD yieldExpr,
            RETURN_CLAUSE returnWithoutGraph,
            WHERE where);

    ADMINISTRATION_COMMAND showUserPrivileges(
            POS p,
            List<SimpleEither<StringPos<POS>, PARAMETER>> users,
            boolean asCommand,
            boolean asRevoke,
            YIELD yieldExpr,
            RETURN_CLAUSE returnWithoutGraph,
            WHERE where);

    ADMINISTRATION_COMMAND grantPrivilege(
            POS p, List<SimpleEither<StringPos<POS>, PARAMETER>> roles, PRIVILEGE_TYPE privilege);

    ADMINISTRATION_COMMAND denyPrivilege(
            POS p, List<SimpleEither<StringPos<POS>, PARAMETER>> roles, PRIVILEGE_TYPE privilege);

    ADMINISTRATION_COMMAND revokePrivilege(
            POS p,
            List<SimpleEither<StringPos<POS>, PARAMETER>> roles,
            PRIVILEGE_TYPE privilege,
            boolean revokeGrant,
            boolean revokeDeny);

    PRIVILEGE_TYPE databasePrivilege(
            POS p,
            ADMINISTRATION_ACTION action,
            DATABASE_SCOPE scope,
            List<PRIVILEGE_QUALIFIER> qualifier,
            boolean immutable);

    PRIVILEGE_TYPE dbmsPrivilege(
            POS p, ADMINISTRATION_ACTION action, List<PRIVILEGE_QUALIFIER> qualifier, boolean immutable);

    PRIVILEGE_TYPE loadPrivilege(
            POS p, SimpleEither<String, PARAMETER> url, SimpleEither<String, PARAMETER> cidr, boolean immutable);

    PRIVILEGE_TYPE graphPrivilege(
            POS p,
            ADMINISTRATION_ACTION action,
            GRAPH_SCOPE scope,
            PRIVILEGE_RESOURCE resource,
            List<PRIVILEGE_QUALIFIER> qualifier,
            boolean immutable);

    ADMINISTRATION_ACTION privilegeAction(ActionType action);

    // Resources

    PRIVILEGE_RESOURCE propertiesResource(POS p, List<String> property);

    PRIVILEGE_RESOURCE allPropertiesResource(POS p);

    PRIVILEGE_RESOURCE labelsResource(POS p, List<String> label);

    PRIVILEGE_RESOURCE allLabelsResource(POS p);

    PRIVILEGE_RESOURCE databaseResource(POS p);

    PRIVILEGE_RESOURCE noResource(POS p);

    PRIVILEGE_QUALIFIER labelQualifier(POS p, String label);

    PRIVILEGE_QUALIFIER allLabelsQualifier(POS p);

    PRIVILEGE_QUALIFIER relationshipQualifier(POS p, String relationshipType);

    PRIVILEGE_QUALIFIER allRelationshipsQualifier(POS p);

    PRIVILEGE_QUALIFIER elementQualifier(POS p, String name);

    PRIVILEGE_QUALIFIER allElementsQualifier(POS p);

    PRIVILEGE_QUALIFIER patternQualifier(
            List<PRIVILEGE_QUALIFIER> qualifiers, VARIABLE variable, EXPRESSION expression);

    List<PRIVILEGE_QUALIFIER> allQualifier();

    List<PRIVILEGE_QUALIFIER> allDatabasesQualifier();

    List<PRIVILEGE_QUALIFIER> userQualifier(List<SimpleEither<StringPos<POS>, PARAMETER>> users);

    List<PRIVILEGE_QUALIFIER> allUsersQualifier();

    List<PRIVILEGE_QUALIFIER> functionQualifier(POS p, List<String> functions);

    List<PRIVILEGE_QUALIFIER> procedureQualifier(POS p, List<String> procedures);

    List<PRIVILEGE_QUALIFIER> settingQualifier(POS p, List<String> names);

    GRAPH_SCOPE graphScope(POS p, List<DATABASE_NAME> graphNames, ScopeType scopeType);

    DATABASE_SCOPE databasePrivilegeScope(POS p, List<DATABASE_NAME> databaseNames, ScopeType scopeType);

    // Server Administration Commands

    ADMINISTRATION_COMMAND enableServer(
            POS p,
            SimpleEither<String, PARAMETER> serverName,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> options);

    ADMINISTRATION_COMMAND alterServer(
            POS p,
            SimpleEither<String, PARAMETER> serverName,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> options);

    ADMINISTRATION_COMMAND renameServer(
            POS p, SimpleEither<String, PARAMETER> serverName, SimpleEither<String, PARAMETER> newName);

    ADMINISTRATION_COMMAND dropServer(POS p, SimpleEither<String, PARAMETER> serverName);

    ADMINISTRATION_COMMAND showServers(POS p, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where);

    ADMINISTRATION_COMMAND deallocateServers(POS p, boolean dryRun, List<SimpleEither<String, PARAMETER>> serverNames);

    ADMINISTRATION_COMMAND reallocateDatabases(POS p, boolean dryRun);

    // Database Administration Commands

    ADMINISTRATION_COMMAND createDatabase(
            POS p,
            boolean replace,
            DATABASE_NAME databaseName,
            boolean ifNotExists,
            WAIT_CLAUSE waitClause,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> options,
            SimpleEither<Integer, PARAMETER> topologyPrimaries,
            SimpleEither<Integer, PARAMETER> topologySecondaries);

    ADMINISTRATION_COMMAND createCompositeDatabase(
            POS p,
            boolean replace,
            DATABASE_NAME compositeDatabaseName,
            boolean ifNotExists,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> options,
            WAIT_CLAUSE waitClause);

    ADMINISTRATION_COMMAND dropDatabase(
            POS p,
            DATABASE_NAME databaseName,
            boolean ifExists,
            boolean composite,
            boolean aliasAction,
            boolean dumpData,
            WAIT_CLAUSE wait);

    ADMINISTRATION_COMMAND alterDatabase(
            POS p,
            DATABASE_NAME databaseName,
            boolean ifExists,
            AccessType accessType,
            SimpleEither<Integer, PARAMETER> topologyPrimaries,
            SimpleEither<Integer, PARAMETER> topologySecondaries,
            Map<String, EXPRESSION> options,
            Set<String> optionsToRemove,
            WAIT_CLAUSE waitClause);

    ADMINISTRATION_COMMAND showDatabase(
            POS p, DATABASE_SCOPE scope, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where);

    ADMINISTRATION_COMMAND startDatabase(POS p, DATABASE_NAME databaseName, WAIT_CLAUSE wait);

    ADMINISTRATION_COMMAND stopDatabase(POS p, DATABASE_NAME databaseName, WAIT_CLAUSE wait);

    DATABASE_SCOPE databaseScope(POS p, DATABASE_NAME databaseName, boolean isDefault, boolean isHome);

    WAIT_CLAUSE wait(boolean wait, long seconds);

    DATABASE_NAME databaseName(POS p, List<String> names);

    DATABASE_NAME databaseName(PARAMETER param);

    // Alias Administration Commands
    ADMINISTRATION_COMMAND createLocalDatabaseAlias(
            POS p,
            boolean replace,
            DATABASE_NAME aliasName,
            DATABASE_NAME targetName,
            boolean ifNotExists,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> properties);

    ADMINISTRATION_COMMAND createRemoteDatabaseAlias(
            POS p,
            boolean replace,
            DATABASE_NAME aliasName,
            DATABASE_NAME targetName,
            boolean ifNotExists,
            SimpleEither<String, PARAMETER> url,
            SimpleEither<StringPos<POS>, PARAMETER> username,
            EXPRESSION password,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> driverSettings,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> properties);

    ADMINISTRATION_COMMAND alterLocalDatabaseAlias(
            POS p,
            DATABASE_NAME aliasName,
            DATABASE_NAME targetName,
            boolean ifExists,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> properties);

    ADMINISTRATION_COMMAND alterRemoteDatabaseAlias(
            POS p,
            DATABASE_NAME aliasName,
            DATABASE_NAME targetName,
            boolean ifExists,
            SimpleEither<String, PARAMETER> url,
            SimpleEither<StringPos<POS>, PARAMETER> username,
            EXPRESSION password,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> driverSettings,
            SimpleEither<Map<String, EXPRESSION>, PARAMETER> properties);

    ADMINISTRATION_COMMAND dropAlias(POS p, DATABASE_NAME aliasName, boolean ifExists);

    ADMINISTRATION_COMMAND showAliases(
            POS p, DATABASE_NAME aliasName, YIELD yieldExpr, RETURN_CLAUSE returnWithoutGraph, WHERE where);

    void addDeprecatedIdentifierUnicodeNotification(POS p, Character character, String identifier);
}
