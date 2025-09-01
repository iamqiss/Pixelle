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
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class CantCompileQueryException extends Neo4jException {
    public CantCompileQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public CantCompileQueryException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    public CantCompileQueryException(String message) {
        super(message);
    }

    public CantCompileQueryException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static CantCompileQueryException unsupportedRuntimeInThisVersion(String runtime) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N27)
                        .withParam(GqlParams.StringParam.item, runtime)
                        .withParam(GqlParams.StringParam.edition, "community edition")
                        .build())
                .build();
        return new CantCompileQueryException(
                gql, String.format("This version of Neo4j does not support the requested runtime: `%s`", runtime));
    }

    public static CantCompileQueryException planNotRecognisedInAdminCommand(String unknownPlan) {
        var msg = String.format("Plan is not a recognized database administration command: %s", unknownPlan);
        var gql = GqlHelper.get50N00(CantCompileQueryException.class.getSimpleName(), msg);
        return new CantCompileQueryException(gql, msg);
    }

    public static CantCompileQueryException planNotSchemaCommand(String unknownPlan) {
        var msg = String.format("Plan is not a schema command: %s", unknownPlan);
        var gql = GqlHelper.get50N00(CantCompileQueryException.class.getSimpleName(), msg);
        return new CantCompileQueryException(gql, msg);
    }

    @Override
    public Status status() {
        return Status.Statement.ExecutionFailed;
    }
}
