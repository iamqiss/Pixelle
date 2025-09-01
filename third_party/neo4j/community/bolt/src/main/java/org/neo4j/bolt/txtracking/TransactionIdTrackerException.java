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
package org.neo4j.bolt.txtracking;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.BookmarkTimeout;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.AbstractDatabase;

public class TransactionIdTrackerException extends GqlRuntimeException implements Status.HasStatus {
    private final Status status;

    @Deprecated
    TransactionIdTrackerException(Status status, String message) {
        this(status, message, null);
    }

    private TransactionIdTrackerException(ErrorGqlStatusObject gqlStatusObject, Status status, String message) {
        this(gqlStatusObject, status, message, null);
    }

    @Deprecated
    TransactionIdTrackerException(Status status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    private TransactionIdTrackerException(
            ErrorGqlStatusObject gqlStatusObject, Status status, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
        this.status = status;
    }

    public static TransactionIdTrackerException unreachableDatabaseVersion(
            AbstractDatabase db, long lastTransactionId, long oldestAcceptableTxId, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N13)
                .withParam(GqlParams.StringParam.db, db.getNamedDatabaseId().name())
                .withParam(GqlParams.StringParam.transactionId1, String.valueOf(oldestAcceptableTxId))
                .withParam(GqlParams.StringParam.transactionId2, String.valueOf(lastTransactionId))
                .build();
        return new TransactionIdTrackerException(
                gql,
                BookmarkTimeout,
                "Database '" + db.getNamedDatabaseId().name() + "' not up to the requested version: "
                        + oldestAcceptableTxId + ". " + "Latest database version is " + lastTransactionId,
                cause);
    }

    @Override
    public Status status() {
        return status;
    }
}
