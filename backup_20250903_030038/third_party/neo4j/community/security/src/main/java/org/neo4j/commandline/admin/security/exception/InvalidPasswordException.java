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
package org.neo4j.commandline.admin.security.exception;

import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class InvalidPasswordException extends GqlRuntimeException implements Status.HasStatus {
    private final Status status;

    public InvalidPasswordException(String message) {
        super(message, false, false);
        this.status = Status.Statement.ArgumentError;
    }

    public InvalidPasswordException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message, false, false);
        this.status = Status.Statement.ArgumentError;
    }

    public static InvalidPasswordException shortPassword(int minLength) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N05)
                .withClassification(ErrorClassification.CLIENT_ERROR)
                .withParam(GqlParams.StringParam.input, "***")
                .withParam(GqlParams.StringParam.context, "password")
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N85)
                        .withClassification(ErrorClassification.CLIENT_ERROR)
                        .withParam(GqlParams.NumberParam.lower, minLength)
                        .build())
                .build();
        return new InvalidPasswordException(
                gql, String.format("A password must be at least %s characters.", minLength));
    }

    @Override
    public Status status() {
        return status;
    }
}
