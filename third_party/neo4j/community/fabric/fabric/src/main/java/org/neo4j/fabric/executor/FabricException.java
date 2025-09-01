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
package org.neo4j.fabric.executor;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.HasQuery;
import org.neo4j.kernel.api.exceptions.Status;

public class FabricException extends GqlRuntimeException implements Status.HasStatus, HasQuery {
    private final Status statusCode;
    private Long queryId;

    private static final String ROUTING_ENABLED_SETTING = GraphDatabaseSettings.routing_enabled.name();

    @Deprecated
    public FabricException(Status statusCode, Throwable cause) {
        super(ErrorMessageHolder.getOldCauseMessage(cause), cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    private FabricException(ErrorGqlStatusObject gqlStatusObject, Status statusCode, Throwable cause) {
        super(gqlStatusObject, ErrorMessageHolder.getOldCauseMessage(cause), cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    @Deprecated
    public FabricException(Status statusCode, String message, Object... parameters) {
        super(String.format(message, parameters));
        this.statusCode = statusCode;
        this.queryId = null;
    }

    public FabricException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Object... parameters) {
        super(gqlStatusObject, String.format(message, parameters));
        this.statusCode = statusCode;
        this.queryId = null;
    }

    @Deprecated
    public FabricException(Status statusCode, String message, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    protected FabricException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
        this.statusCode = statusCode;
        this.queryId = null;
    }

    @Deprecated
    public FabricException(Status statusCode, String message, Throwable cause, Long queryId) {
        super(message, cause);
        this.statusCode = statusCode;
        this.queryId = queryId;
    }

    public FabricException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Throwable cause, Long queryId) {
        super(gqlStatusObject, message, cause);
        this.statusCode = statusCode;
        this.queryId = queryId;
    }

    private <T extends Throwable & Status.HasStatus> FabricException(ErrorGqlStatusObject gqlStatusObject, T cause) {
        super(gqlStatusObject, cause.getMessage(), cause);
        this.statusCode = cause.status();
    }

    public static <T extends Exception & Status.HasStatus> FabricException translateLocalError(T localException) {
        if (localException instanceof ErrorGqlStatusObject gqlException && gqlException.gqlStatusObject() != null) {
            return new FabricException(gqlException, localException);
        }
        return new FabricException(localException.status(), localException.getMessage(), localException);
    }

    public static FabricException noLeaderAddress(String dbName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N00)
                .withParam(GqlParams.StringParam.db, dbName)
                .build();
        return new FabricException(
                gql,
                Status.Cluster.NotALeader,
                "Unable to route to database '%s'. Unable to get bolt address of leader.",
                dbName);
    }

    public static FabricException sessionDbNotLeader(String dbName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N01)
                .withParam(GqlParams.StringParam.db, dbName)
                .withParam(GqlParams.StringParam.cfgSetting, ROUTING_ENABLED_SETTING)
                .build();
        return new FabricException(
                gql,
                Status.Cluster.NotALeader,
                String.format(
                        """
                        No longer possible to write to database '%s' on this instance and unable to route write operation to leader. Server-side routing is disabled.
                        Either connect to the database directly using the driver (or interactively with the :use command),
                        or enable server-side routing by setting `%s=true`""",
                        dbName, ROUTING_ENABLED_SETTING),
                dbName);
    }

    public static FabricException routingDisabled(String dbName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N02)
                .withParam(GqlParams.StringParam.db, dbName)
                .withParam(GqlParams.StringParam.cfgSetting, ROUTING_ENABLED_SETTING)
                .build();
        return new FabricException(
                gql,
                Status.Cluster.Routing,
                String.format(
                        """
         Unable to route to database '%s'. Server-side routing is disabled.
         Either connect to the database directly using the driver (or interactively with the :use command),
         or enable server-side routing by setting `%s=true`""",
                        dbName, ROUTING_ENABLED_SETTING),
                dbName);
    }

    public static FabricException failedToParseBookmark(Exception exception) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N12)
                .build();
        return new FabricException(
                gql,
                InvalidBookmark,
                "Parsing of supplied bookmarks failed with message: " + exception.getMessage(),
                exception);
    }

    public static FabricException writeDuringLeaderSwitch(Location attempt, Location current) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N34)
                .build();
        return new FabricException(
                gql,
                Status.Transaction.LeaderSwitch,
                "Could not write to a database due to a cluster leader switch that occurred during the transaction. "
                        + "Previous leader: %s, Current leader: %s.",
                current,
                attempt);
    }

    public static FabricException databaseLocationChanged(String dbName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N35)
                .withParam(GqlParams.StringParam.db, dbName)
                .build();
        return new FabricException(
                gql,
                Status.Transaction.Outdated,
                "The locations associated with the graph name %s have " + "changed whilst the transaction was running.",
                dbName);
    }

    public static FabricException executeQueryInClosedTransaction() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N07)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N05)
                        .build())
                .build();
        return new FabricException(
                gql, Status.Statement.ExecutionFailed, "Trying to execute query in a closed transaction");
    }

    public static FabricException invalidCombinationOfStatementTypes(String query, String message) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N01)
                .withParam(GqlParams.StringParam.query, query)
                .build();
        return new FabricException(gql, Status.Transaction.ForbiddenDueToTransactionType, message);
    }

    public static FabricException transactionCommitFailed(Status statusCode, String message) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN01)
                .build();
        return new FabricException(gql, statusCode, message);
    }

    public static FabricException transactionRollbackFailed(Status statusCode, String message) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_40N01)
                .build();
        return new FabricException(gql, statusCode, message);
    }

    public static FabricException transactionTerminationFailed(Status statusCode, String message) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_2DN03)
                .build();
        return new FabricException(gql, statusCode, message);
    }

    @Override
    public Status status() {
        return statusCode;
    }

    @Override
    public Long query() {
        return queryId;
    }

    @Override
    public void setQuery(Long queryId) {
        this.queryId = queryId;
    }
}
