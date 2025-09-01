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
package org.neo4j.packstream.error.struct;

import static java.lang.String.format;

import java.util.Arrays;
import java.util.List;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.error.reader.PackstreamReaderException;

public class IllegalStructArgumentException extends PackstreamStructException {
    private final String fieldName;

    private IllegalStructArgumentException(String fieldName, PackstreamReaderException cause) {
        // In case of Packstream exceptions, we'll copy the cause message in order to make it available to the client
        // as well - in all other cases this information will be suppressed as we do not wish to accidentally leak any
        // information that could provide information about internal processes
        super(String.format("Illegal value for field \"%s\": %s", fieldName, cause.getMessage()), cause);
        this.fieldName = fieldName;
    }

    private IllegalStructArgumentException(
            ErrorGqlStatusObject gqlStatusObject, String fieldName, PackstreamReaderException cause) {
        // In case of Packstream exceptions, we'll copy the cause message in order to make it available to the client
        // as well - in all other cases this information will be suppressed as we do not wish to accidentally leak any
        // information that could provide information about internal processes
        super(
                gqlStatusObject,
                String.format("Illegal value for field \"%s\": %s", fieldName, cause.getMessage()),
                cause);
        this.fieldName = fieldName;
    }

    public static IllegalStructArgumentException protocolError(String fieldName, PackstreamReaderException cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N06)
                .build();
        return new IllegalStructArgumentException(gql, fieldName, cause);
    }

    private IllegalStructArgumentException(String fieldName, Throwable cause) {
        super(String.format("Illegal value for field \"%s\"", fieldName), cause);
        this.fieldName = fieldName;
    }

    private IllegalStructArgumentException(ErrorGqlStatusObject gqlStatusObject, String fieldName, Throwable cause) {
        super(gqlStatusObject, String.format("Illegal value for field \"%s\"", fieldName), cause);

        this.fieldName = fieldName;
    }

    public static IllegalStructArgumentException invalidInput(
            String fieldName, String input, String context, List<String> expectedInputList, Throwable cause) {
        return new IllegalStructArgumentException(
                GqlHelper.getGql08N06_22N04(input, context, expectedInputList), fieldName, cause);
    }

    public IllegalStructArgumentException(String fieldName, String message, Throwable cause) {
        super(String.format("Illegal value for field \"%s\": %s", fieldName, message), cause);
        this.fieldName = fieldName;
    }

    public IllegalStructArgumentException(
            ErrorGqlStatusObject gqlStatusObject, String fieldName, String message, Throwable cause) {
        super(gqlStatusObject, String.format("Illegal value for field \"%s\": %s", fieldName, message), cause);

        this.fieldName = fieldName;
    }

    public static IllegalStructArgumentException invalidCoordinateArguments(
            String fieldName, String valueType, double[] coordinates, String message, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N06)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N24)
                        .withParam(GqlParams.StringParam.valueType, valueType)
                        .withParam(GqlParams.StringParam.coordinates, Arrays.toString(coordinates))
                        .build())
                .build();
        return new IllegalStructArgumentException(gql, fieldName, message, cause);
    }

    public static IllegalStructArgumentException invalidTemporalComponent(
            String fieldName, long epochSecond, long nanos, Throwable cause) {
        ErrorGqlStatusObject gqlCause = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N15)
                .withParam(GqlParams.StringParam.component, fieldName)
                .withParam(GqlParams.StringParam.temporal, epochSecond + "+" + nanos)
                .build();
        return new IllegalStructArgumentException(
                GqlHelper.getGql08N06(gqlCause),
                fieldName,
                format("Illegal epoch adjustment epoch seconds: %d+%d", epochSecond, nanos),
                cause);
    }

    // Make this constructor private once the different PRs are merged
    public IllegalStructArgumentException(String fieldName, String message) {
        this(fieldName, message, null);
    }

    // Make this constructor private once the different PRs are merged
    public IllegalStructArgumentException(ErrorGqlStatusObject gqlStatusObject, String fieldName, String message) {
        this(gqlStatusObject, fieldName, message, null);
    }

    public static IllegalStructArgumentException wrongTypeForFieldName(
            String fieldName, String value, List<String> expectedType, String actualType, String message) {
        return new IllegalStructArgumentException(
                GqlHelper.getGql08N06(GqlHelper.getGql22G03_22N01(value, expectedType, actualType)),
                fieldName,
                message);
    }

    public static IllegalStructArgumentException wrongTypeForFieldNameOrOutOfRange(
            String fieldName,
            String expectedType,
            Number lowerLimit,
            Number upperLimit,
            Number actualValue,
            String message) {
        var gql = GqlHelper.getGql08N06(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N03)
                .withParam(GqlParams.StringParam.component, fieldName)
                .withParam(GqlParams.StringParam.valueType, expectedType)
                .withParam(GqlParams.NumberParam.lower, lowerLimit)
                .withParam(GqlParams.NumberParam.upper, upperLimit)
                .withParam(GqlParams.StringParam.value, String.valueOf(actualValue))
                .build());
        return new IllegalStructArgumentException(gql, fieldName, message);
    }

    public static IllegalStructArgumentException invalidInput(
            String fieldName, String input, String context, List<String> expectedInputList, String message) {
        return new IllegalStructArgumentException(
                GqlHelper.getGql08N06_22N04(input, context, expectedInputList), fieldName, message);
    }

    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public Status status() {
        // When we're wrapping another Packstream related exception which bears its own status, we'll take over the
        // original status code instead.
        var cause = this.getCause();
        if (cause instanceof PackstreamReaderException && cause instanceof Status.HasStatus) {
            return ((Status.HasStatus) cause).status();
        }

        return Status.Request.Invalid;
    }
}
