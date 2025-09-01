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

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.ErrorMessageHolder;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class LimitExceededException extends PackstreamReaderException
        implements Status.HasStatus, ErrorGqlStatusObject {
    private final long limit;
    private final long actual;

    @Deprecated
    protected LimitExceededException(long limit, long actual) {
        super("Value of size " + actual + " exceeded limit of " + limit);

        this.limit = limit;
        this.actual = actual;
    }

    protected LimitExceededException(ErrorGqlStatusObject gqlStatusObject, long limit, long actual) {
        super(
                gqlStatusObject,
                ErrorMessageHolder.getMessage(
                        gqlStatusObject, "Value of size " + actual + " exceeded limit of " + limit));

        this.limit = limit;
        this.actual = actual;
    }

    public static LimitExceededException protocolMessageLengthLimitOverflow(long limit, long actual) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N56)
                .withParam(GqlParams.NumberParam.boltMsgLenLimit, limit)
                .build();
        return new LimitExceededException(gql, limit, actual);
    }

    public long getLimit() {
        return this.limit;
    }

    public long getActual() {
        return this.actual;
    }

    @Override
    public Status status() {
        return Status.Request.Invalid;
    }
}
