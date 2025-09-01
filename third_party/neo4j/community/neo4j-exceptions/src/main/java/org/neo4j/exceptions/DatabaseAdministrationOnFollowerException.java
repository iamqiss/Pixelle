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
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class DatabaseAdministrationOnFollowerException extends CypherExecutionException {

    private static final String FOLLOWER_ERROR = "Administration commands must be executed on the LEADER server.";

    private DatabaseAdministrationOnFollowerException(
            ErrorGqlStatusObject gqlStatusObject, String message, Throwable cause) {
        super(gqlStatusObject, message, cause);
    }

    public static DatabaseAdministrationOnFollowerException notALeader(
            String command, String startOfLegacyMessage, Throwable cause) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N29)
                .withParam(GqlParams.StringParam.cmd, command)
                .build();

        return new DatabaseAdministrationOnFollowerException(
                gql, String.format("%s: %s", startOfLegacyMessage, FOLLOWER_ERROR), cause);
    }

    @Override
    public Status status() {
        return Status.Cluster.NotALeader;
    }
}
