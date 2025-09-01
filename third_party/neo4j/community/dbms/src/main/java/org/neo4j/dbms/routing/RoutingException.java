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
package org.neo4j.dbms.routing;

import static java.lang.String.format;

import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlException;
import org.neo4j.gqlstatus.GqlParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;

public class RoutingException extends GqlException implements Status.HasStatus {
    private final Status status;

    @Deprecated
    public RoutingException(Status status, String message) {
        super(message);
        this.status = status;
    }

    public RoutingException(ErrorGqlStatusObject gqlStatusObject, Status status, String message) {
        super(gqlStatusObject, message);
        this.status = status;
    }

    public static RoutingException policyDefinitionNotFound(String policyName) {
        var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_08N15)
                .withParam(GqlParams.StringParam.routingPolicy, policyName)
                .build();
        return new RoutingException(
                gql,
                Status.Routing.RoutingFailed,
                format("Policy definition for '%s' could not be found.", policyName));
    }

    @Override
    public Status status() {
        return status;
    }
}
