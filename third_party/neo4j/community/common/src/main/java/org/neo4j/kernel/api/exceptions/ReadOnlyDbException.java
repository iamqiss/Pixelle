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
package org.neo4j.kernel.api.exceptions;

import org.neo4j.exceptions.KernelException;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;

/**
 * This exception is thrown when committing an updating transaction in a read only database. Can also be thrown when
 * trying to create tokens (like new property names), ids, indexes files in a read only database.
 */
public class ReadOnlyDbException extends KernelException {

    private ReadOnlyDbException(ErrorGqlStatusObject gqlStatusObject) {
        super(
                gqlStatusObject,
                Status.General.ForbiddenOnReadOnlyDatabase,
                "This Neo4j instance is read only for all databases");
    }

    public static ReadOnlyDbException databaseIsReadOnly() {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N08)
                .build();
        return new ReadOnlyDbException(gql);
    }
}
