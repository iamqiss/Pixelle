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
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class InternalException extends Neo4jException {
    @Deprecated
    public InternalException(String message, Throwable cause) {
        super(message, cause);
    }

    private InternalException(ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    @Deprecated
    public InternalException(String message) {
        super(message);
    }

    protected InternalException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static InternalException foundNoSolutionForBlock(int blockSize, String blockCandidates, String table) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N24)
                .build();
        return new InternalException(
                gql,
                String.format(
                        """
                                Found no solution for block with size %d,
                                |%s were the selected candidates from the table %s""",
                        blockSize, blockCandidates, table));
    }

    public static InternalException foundNoPlanWithinConstraints(String setting1, String setting2) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N24)
                .build();
        return new InternalException(
                gql,
                String.format(
                        """
             Unfortunately, the planner was unable to find a plan within the constraints provided.
             |Try increasing the config values `%s`
             |and `%s` to allow
             |for a larger sub-plan table and longer planning time.""",
                        setting1, setting2));
    }

    @Override
    public Status status() {
        return Status.Statement.ExecutionFailed;
    }
}
