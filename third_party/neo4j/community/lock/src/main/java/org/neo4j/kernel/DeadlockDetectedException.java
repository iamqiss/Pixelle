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
package org.neo4j.kernel;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Signals that a deadlock between two or more transactions has been detected.
 */
public class DeadlockDetectedException extends TransientTransactionFailureException {
    @Deprecated
    public DeadlockDetectedException(String message) {
        super(Status.Transaction.DeadlockDetected, message);
    }

    private DeadlockDetectedException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, Status.Transaction.DeadlockDetected, message);
    }

    public DeadlockDetectedException(String message, Throwable cause) {
        super(Status.Transaction.DeadlockDetected, message, cause);
    }

    private DeadlockDetectedException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, Status.Transaction.DeadlockDetected, message, cause);
    }

    public static DeadlockDetectedException deadlockDetected(String legacyMessage) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_50N05)
                .build();

        return new DeadlockDetectedException(gql, legacyMessage);
    }
}
