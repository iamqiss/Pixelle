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
package org.neo4j.kernel.api.security.exception;

import static java.lang.String.format;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class InvalidAuthTokenException extends GqlException implements Status.HasStatus {
    private final Status status;

    private static final String THE_VALUE_ASSOCIATED_WITH_THE_PARAMETER_TEMPLATE =
            "The value associated with the parameter %s must be a %s but was: %s";

    private static final String PARAMETER_NOT_PROVIDED_TEMPLATE = "'%s' parameter not provided";

    private static final String WRONG_TYPE_TEMPLATE = "%s must be a %s but was: %s";

    private InvalidAuthTokenException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
        this.status = Status.Security.Unauthorized;
    }

    public static InvalidAuthTokenException unsupportedAuthenticationToken(String explanation) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidAuthTokenException(gql, format("Unsupported authentication token%s", explanation));
    }

    public static InvalidAuthTokenException parameterNotProvided(String parameter) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidAuthTokenException(gql, String.format(PARAMETER_NOT_PROVIDED_TEMPLATE, parameter));
    }

    public static InvalidAuthTokenException wrongTypeForParameter(
            String parameter, String expectedType, String actualType) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidAuthTokenException(
                gql,
                String.format(THE_VALUE_ASSOCIATED_WITH_THE_PARAMETER_TEMPLATE, parameter, expectedType, actualType));
    }

    public static InvalidAuthTokenException wrongType(String parameter, String expectedType, String actualType) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidAuthTokenException(
                gql, String.format(WRONG_TYPE_TEMPLATE, parameter, expectedType, actualType));
    }

    public static InvalidAuthTokenException valueMustBeMap(String key, String valueType) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidAuthTokenException(
                gql, "The value associated with the key `" + key + "` must be a Map but was: " + valueType);
    }

    public static InvalidAuthTokenException tokenError(String token, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42NFF)
                .build();
        return new InvalidAuthTokenException(gql, cause.getMessage() + ": " + token);
    }

    @Override
    public Status status() {
        return status;
    }
}
