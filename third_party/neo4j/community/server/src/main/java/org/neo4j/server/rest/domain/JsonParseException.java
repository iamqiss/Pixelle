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
package org.neo4j.server.rest.domain;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.GqlException;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

@SuppressWarnings("serial")
public class JsonParseException extends GqlException implements Status.HasStatus {

    public JsonParseException(String message, Throwable cause) {
        super(message, cause);
    }

    public JsonParseException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    public static JsonParseException jsonParsingException(int line, int column, String message, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N11)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N95)
                        .atPosition(line, column, -1)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA8)
                                .withParam(
                                        GqlParams.StringParam.cause,
                                        String.format("%s [line: %d, column: %d]", message, line, column))
                                .build())
                        .build())
                .build();
        return new JsonParseException(gql, message, cause);
    }

    public JsonParseException(Throwable cause) {
        super(ErrorMessageHolder.getOldCauseMessage(cause), cause);
    }

    public JsonParseException(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        super(gqlStatusObject, ErrorMessageHolder.getOldCauseMessage(cause), cause);
    }

    public static JsonParseException jsonParsingException(Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N11)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N95)
                        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA8)
                                .withParam(
                                        GqlParams.StringParam.cause,
                                        String.format(ErrorMessageHolder.getOldCauseMessage(cause)))
                                .build())
                        .build())
                .build();
        return new JsonParseException(gql, cause);
    }

    @Override
    public Status status() {
        return Status.Request.InvalidFormat;
    }
}
