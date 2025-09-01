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
package org.neo4j.internal.kernel.api.exceptions;

import static org.neo4j.kernel.api.exceptions.Status.Cluster.ReplicationFailure;
import static org.neo4j.kernel.api.exceptions.Status.General.UnknownError;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LeaseExpired;

import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class TransactionFailureException extends KernelException {
    @Deprecated
    public TransactionFailureException(Status statusCode, Throwable cause, String message, Object... parameters) {
        super(statusCode, cause, message, parameters);
    }

    protected TransactionFailureException(
            ErrorGqlStatusObject gqlStatusObject,
            Status statusCode,
            Throwable cause,
            String message,
            Object... parameters) {
        super(gqlStatusObject, statusCode, cause, message, parameters);
    }

    @Deprecated
    public TransactionFailureException(Status statusCode, Throwable cause) {
        super(statusCode, cause);
    }

    public TransactionFailureException(ErrorGqlStatusObject gqlStatusObject, Status statusCode, Throwable cause) {
        super(gqlStatusObject, statusCode, cause);
    }

    @Deprecated
    public TransactionFailureException(Status statusCode, String message, Object... parameters) {
        super(statusCode, message, parameters);
    }

    public TransactionFailureException(
            ErrorGqlStatusObject gqlStatusObject, Status statusCode, String message, Object... parameters) {
        super(gqlStatusObject, statusCode, message, parameters);
    }

    // To satisfy DatabaseHealth
    @Deprecated
    public TransactionFailureException(String message, Throwable cause) {
        super(Status.Transaction.TransactionStartFailed, cause, message);
    }

    private TransactionFailureException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, Status.Transaction.TransactionStartFailed, cause, message);
    }

    public static TransactionFailureException leaseExpired(int currentLeaseId, int leaseId) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N08)
                .build();
        return new TransactionFailureException(
                gql,
                LeaseExpired,
                "The lease used for the transaction has expired: [current lease id:%d, transaction lease id:%d]",
                currentLeaseId,
                leaseId);
    }

    public static TransactionFailureException invalidatedLease() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N08)
                .build();
        return new TransactionFailureException(gql, LeaseExpired, "The lease has been invalidated");
    }

    public static TransactionFailureException unexpectedOutcome(String outcome) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_25N09)
                .build();
        return new TransactionFailureException(gql, UnknownError, "Unexpected outcome: " + outcome);
    }

    public static TransactionFailureException replicationError(Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N33)
                .build();
        return new TransactionFailureException(gql, ReplicationFailure, cause);
    }
}
