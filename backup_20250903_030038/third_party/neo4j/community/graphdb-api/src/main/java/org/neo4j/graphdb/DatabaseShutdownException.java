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
package org.neo4j.graphdb;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class DatabaseShutdownException extends GqlRuntimeException implements Status.HasStatus {
    private static final String MESSAGE = "This database is shutdown.";

    @Deprecated
    public DatabaseShutdownException() {
        super(MESSAGE);
    }

    public DatabaseShutdownException(ErrorGqlStatusObject gqlStatusObject) {
        super(gqlStatusObject, MESSAGE);
    }

    @Deprecated
    public DatabaseShutdownException(Throwable cause) {
        super(MESSAGE, cause);
    }

    public DatabaseShutdownException(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        super(gqlStatusObject, MESSAGE, cause);
    }

    public static DatabaseShutdownException databaseUnavailable(String dbName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlParams.StringParam.db, dbName)
                .build();
        return new DatabaseShutdownException(gql);
    }

    public static DatabaseShutdownException databaseUnavailable(String dbName, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N09)
                .withParam(GqlParams.StringParam.db, dbName)
                .build();
        return new DatabaseShutdownException(gql, cause);
    }

    @Override
    public Status status() {
        return Status.General.DatabaseUnavailable;
    }
}
