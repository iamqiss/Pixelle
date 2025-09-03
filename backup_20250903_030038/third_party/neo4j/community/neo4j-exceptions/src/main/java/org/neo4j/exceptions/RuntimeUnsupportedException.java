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
package org.neo4j.exceptions;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class RuntimeUnsupportedException extends Neo4jException {
    @Deprecated
    public RuntimeUnsupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    private RuntimeUnsupportedException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    @Deprecated
    public RuntimeUnsupportedException(String message) {
        super(message);
    }

    private RuntimeUnsupportedException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static RuntimeUnsupportedException unsupportedRuntimeInThisVersion(String runtime) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N27)
                        .withParam(GqlParams.StringParam.item, runtime)
                        .withParam(GqlParams.StringParam.edition, "community edition")
                        .build())
                .build();
        return new RuntimeUnsupportedException(
                gql, String.format("This version of Neo4j does not support the requested runtime: `%s`", runtime));
    }

    public static RuntimeUnsupportedException invalidParallelRuntimeConfiguration(String settingName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N47)
                        .build())
                .build();
        return new RuntimeUnsupportedException(
                gql,
                "There are no workers configured for the parallel runtime. "
                        + String.format("Change '%s' to some larger value to use the parallel runtime.", settingName));
    }

    public static RuntimeUnsupportedException parallelRuntimeWrite() {
        return new RuntimeUnsupportedException(
                unsupportedParallelRuntimeGqlStatusObject(),
                "The parallel runtime does not support updating queries. Please use another runtime");
    }

    public static RuntimeUnsupportedException parallelRuntimeChangesInTransactionState() {
        return new RuntimeUnsupportedException(
                unsupportedParallelRuntimeGqlStatusObject(),
                "The parallel runtime is not supported if there are changes in the transaction state. Use another runtime.");
    }

    private static ErrorGqlStatusObject unsupportedParallelRuntimeGqlStatusObject() {
        return ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N46)
                        .build())
                .build();
    }

    @Override
    public Status status() {
        return Status.Statement.RuntimeUnsupportedError;
    }
}
