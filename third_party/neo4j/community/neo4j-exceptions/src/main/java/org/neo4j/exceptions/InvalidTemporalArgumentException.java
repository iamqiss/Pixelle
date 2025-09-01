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
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

public class InvalidTemporalArgumentException extends InvalidArgumentException {

    @Deprecated
    public InvalidTemporalArgumentException(String message) {
        super(message);
    }

    public InvalidTemporalArgumentException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static InvalidTemporalArgumentException namedTimeZoneWithoutDate() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22007)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N13)
                        .build())
                .build();
        return new InvalidTemporalArgumentException(
                gql,
                "Using a named time zone e.g. [UTC] is not valid for a time without a date. Instead, use a specific time zone string e.g. +00:00.");
    }

    public static InvalidTemporalArgumentException invalidOffset(String offset) {
        var gql = GqlHelper.getGql22007_22N12(offset);
        return new InvalidTemporalArgumentException(gql, "Not a valid offset: " + offset);
    }
}
