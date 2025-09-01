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

public class IndexWithNameAlreadyExistsException extends SchemaRuleWithNameAlreadyExistsException {
    private static final String INDEX_NAME_FORMAT = "There already exists an index called '%s'.";

    private IndexWithNameAlreadyExistsException(ErrorGqlStatusObject gqlStatusObject, String schemaName) {
        super(gqlStatusObject, Status.Schema.IndexWithNameAlreadyExists, String.format(INDEX_NAME_FORMAT, schemaName));
    }

    public static IndexWithNameAlreadyExistsException duplicateIndexName(String name) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N71)
                .withParam(GqlParams.StringParam.idx, name)
                .build();
        return new IndexWithNameAlreadyExistsException(gql, name);
    }
}
