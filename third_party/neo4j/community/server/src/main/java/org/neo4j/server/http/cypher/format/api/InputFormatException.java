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
package org.neo4j.server.http.cypher.format.api;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

/**
 * An exception that should be thrown when input cannot be deserialized.
 * <p>
 * This exception serves for format problems. {@link ConnectionException} should be used for networking problems.
 */
public class InputFormatException extends RuntimeException implements ErrorGqlStatusObject {
    private final ErrorGqlStatusObject gqlStatusObject;
    private final String oldMessage;

    public InputFormatException(String message, Throwable cause) {
        super(message, cause);

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public InputFormatException(ErrorGqlStatusObject errorGqlStatusObject, String message, Throwable cause) {
        super(message, cause);

        this.gqlStatusObject = errorGqlStatusObject;
        this.oldMessage = message;
    }

    public static InputFormatException jsonParingException(String message, Throwable cause) {
        return new InputFormatException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N11)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N95)
                                .build())
                        .build(),
                message,
                cause);
    }

    public static InputFormatException jsonMappingException(String message, Throwable cause) {
        return new InputFormatException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N11)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N96)
                                .build())
                        .build(),
                message,
                cause);
    }

    @Deprecated
    private InputFormatException(String message) {
        super(message);

        this.gqlStatusObject = null;
        this.oldMessage = message;
    }

    public InputFormatException(ErrorGqlStatusObject errorGqlStatusObject, String message) {
        super(message);

        this.gqlStatusObject = errorGqlStatusObject;
        this.oldMessage = message;
    }

    public static InputFormatException emptyInputString(String requiredOption, String message) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N11)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N06)
                        .build())
                .build();
        throw new InputFormatException(gql, message);
    }

    public static InputFormatException wrongFirstFieldDuringDeserialization(String expectedField, String actualValue) {
        return new InputFormatException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N11)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N98)
                                .withParam(GqlParams.StringParam.field, expectedField)
                                .withParam(GqlParams.StringParam.value, actualValue)
                                .build())
                        .build(),
                String.format(
                        "Unable to deserialize request. " + "Expected first field to be '%s', but was '%s'.",
                        expectedField, actualValue));
    }

    public static InputFormatException wrongTokenDuringDeserialization(String expectedTokens, String foundTokens) {
        return new InputFormatException(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N11)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N99)
                                .withParam(GqlParams.StringParam.token, expectedTokens)
                                .withParam(GqlParams.StringParam.value, foundTokens)
                                .build())
                        .build(),
                String.format(
                        "Unable to deserialize request. " + "Expected %s, found %s.", expectedTokens, foundTokens));
    }

    @Override
    public String legacyMessage() {
        return oldMessage;
    }

    @Override
    public ErrorGqlStatusObject gqlStatusObject() {
        return gqlStatusObject;
    }
}
