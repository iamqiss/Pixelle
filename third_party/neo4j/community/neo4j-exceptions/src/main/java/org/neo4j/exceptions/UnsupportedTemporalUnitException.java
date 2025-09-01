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

import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

/**
 * {@code UnsupportedTemporalUnitException} is thrown if trying to get or assign a temporal unit
 * which is not supported for the current temporal type. Examples of such cases include trying to
 * assign a month to a {@code TimeValue}, trying to truncate a {@code DateValue} to minutes and
 * trying to get the timezone of a {@code LocalDateTimeValue}.
 */
public class UnsupportedTemporalUnitException extends CypherTypeException {
    public UnsupportedTemporalUnitException(String errorMsg) {
        super(errorMsg);
    }

    public UnsupportedTemporalUnitException(ErrorGqlStatusObject gqlStatusObject, String errorMsg) {
        super(gqlStatusObject, errorMsg);
    }

    public UnsupportedTemporalUnitException(String errorMsg, Throwable cause) {
        super(errorMsg, cause);
    }

    public UnsupportedTemporalUnitException(ErrorGqlStatusObject gqlStatusObject, String errorMsg, Throwable cause) {
        super(gqlStatusObject, errorMsg, cause);
    }

    public static UnsupportedTemporalUnitException cannotProcess(String value, Throwable e) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22000)
                .withClassification(ErrorClassification.CLIENT_ERROR)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N11)
                        .withClassification(ErrorClassification.CLIENT_ERROR)
                        .withParam(GqlParams.StringParam.input, value)
                        .build())
                .build();
        return new UnsupportedTemporalUnitException(gql, e.getMessage(), e);
    }
}
