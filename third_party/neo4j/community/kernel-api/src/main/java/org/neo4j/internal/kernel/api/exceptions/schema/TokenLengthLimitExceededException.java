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
package org.neo4j.internal.kernel.api.exceptions.schema;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlRuntimeException;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public final class TokenLengthLimitExceededException extends GqlRuntimeException implements Status.HasStatus {

    public TokenLengthLimitExceededException(String tokenName, String tokenType, int maxLength) {
        this(
                ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I50)
                        .withParam(GqlParams.StringParam.input, tokenName)
                        .withParam(GqlParams.StringParam.tokenType, tokenType)
                        .withParam(GqlParams.NumberParam.value, maxLength)
                        .build(),
                tokenName,
                tokenType,
                maxLength);
    }

    public TokenLengthLimitExceededException(
            ErrorGqlStatusObject errorGqlStatusObject, String tokenName, String tokenType, int maxLength) {
        super(errorGqlStatusObject, getMessage(tokenName, tokenType, maxLength));
    }

    private static String getMessage(String tokenName, String tokenType, int maxLength) {
        return String.format(
                "Invalid input %s... A %s name cannot be longer than %d.",
                tokenName.substring(0, Math.min(tokenName.length(), 100)), tokenType, maxLength);
    }

    @Override
    public Status status() {
        return Status.Schema.TokenLengthError;
    }
}
