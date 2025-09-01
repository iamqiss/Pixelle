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
package org.neo4j.packstream.error.reader;

import java.util.Set;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.packstream.error.PackstreamException;

public class PackstreamReaderException extends PackstreamException {

    public PackstreamReaderException() {}

    public PackstreamReaderException(ErrorGqlStatusObject gqlStatusObject) {
        super(gqlStatusObject);
    }

    public PackstreamReaderException(String message) {
        super(message);
    }

    public PackstreamReaderException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static PackstreamReaderException duplicateMapKey(String key) {
        // DRI-003 (When it gets wrapped in an IllegalStructArgumentException
        // it will get the GQL code 08N06 with this (22N54) as a cause)
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N54)
                .withParam(GqlParams.StringParam.mapKey, key)
                .build();
        return new PackstreamReaderException(gql, "Duplicate map key: \"" + key + "\"");
    }

    public static PackstreamReaderException unknownDriverInterfaceType(long type, Set<Long> expectedType) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N00)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N01)
                        .withParam(GqlParams.StringParam.value, "driver interface type")
                        .withParam(
                                GqlParams.ListParam.valueTypeList,
                                expectedType.stream().toList())
                        .withParam(GqlParams.StringParam.valueType, String.valueOf(type))
                        .build())
                .build();
        return new PackstreamReaderException(gql, "Unknown driver interface type " + type);
    }

    public PackstreamReaderException(String message, Throwable cause) {
        super(message, cause);
    }

    public PackstreamReaderException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    public PackstreamReaderException(Throwable cause) {
        super(cause);
    }

    public PackstreamReaderException(ErrorGqlStatusObject gqlStatusObject, Throwable cause) {
        super(gqlStatusObject, cause);
    }
}
