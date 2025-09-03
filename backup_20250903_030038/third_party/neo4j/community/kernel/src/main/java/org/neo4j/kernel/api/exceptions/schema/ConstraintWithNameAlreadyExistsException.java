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
package org.neo4j.kernel.api.exceptions.schema;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class ConstraintWithNameAlreadyExistsException extends SchemaRuleWithNameAlreadyExistsException {
    private static final String CONSTRAINT_NAME_FORMAT = "There already exists a constraint called '%s'.";

    private ConstraintWithNameAlreadyExistsException(ErrorGqlStatusObject gqlStatusObject, String schemaName) {
        super(
                gqlStatusObject,
                Status.Schema.ConstraintWithNameAlreadyExists,
                String.format(CONSTRAINT_NAME_FORMAT, schemaName));
    }

    public static ConstraintWithNameAlreadyExistsException duplicatedConstraintName(String constraintName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N67)
                .withParam(GqlParams.StringParam.constr, constraintName)
                .build();
        return new ConstraintWithNameAlreadyExistsException(gql, constraintName);
    }
}
