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

import java.util.List;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * The hint in the query does not make sense in itself, regardless of availability of indexes.
 */
public class InvalidHintException extends Neo4jException {
    private InvalidHintException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static InvalidHintException cannotUseTextIndexHint(
            String legacyMessage, String hint, String entity, String variable) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N76)
                .withParam(GqlParams.ListParam.hintList, List.of(hint))
                .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N77)
                        .withParam(GqlParams.StringParam.hint, hint)
                        .withParam(GqlParams.StringParam.entityType, entity)
                        .withParam(GqlParams.StringParam.variable, variable)
                        .build())
                .build();
        return new InvalidHintException(gql, legacyMessage);
    }

    @Override
    public Status status() {
        return Status.Statement.SemanticError;
    }
}
