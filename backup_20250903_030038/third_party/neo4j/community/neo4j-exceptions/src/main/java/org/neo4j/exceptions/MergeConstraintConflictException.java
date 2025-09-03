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

import static java.lang.String.format;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class MergeConstraintConflictException extends Neo4jException {
    private MergeConstraintConflictException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    @Override
    public Status status() {
        return Status.Schema.ConstraintValidationFailed;
    }

    public static MergeConstraintConflictException nodeConflict(String node) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N41)
                        .withParam(GqlParams.StringParam.variable, node)
                        .build())
                .build();
        return new MergeConstraintConflictException(
                gql,
                format(
                        "Merge did not find a matching node %s and can not create a new node due to conflicts with existing unique nodes",
                        node));
    }

    public static MergeConstraintConflictException relationshipConflict(String relationship) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22G03)
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N42)
                        .withParam(GqlParams.StringParam.variable, relationship)
                        .build())
                .build();
        return new MergeConstraintConflictException(
                gql,
                format(
                        "Merge did not find a matching relationship %s and can not create a new relationship due to conflicts with existing unique relationships",
                        relationship));
    }
}
