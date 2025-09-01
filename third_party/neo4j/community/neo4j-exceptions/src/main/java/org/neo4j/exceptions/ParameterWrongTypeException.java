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
import org.neo4j.gqlstatus.GqlHelper;
import org.neo4j.kernel.api.exceptions.Status;

public class ParameterWrongTypeException extends Neo4jException {

    public ParameterWrongTypeException(String message) {
        super(message);
    }

    public ParameterWrongTypeException(ErrorGqlStatusObject gqlStatusObject, String message) {
        super(gqlStatusObject, message);
    }

    public static ParameterWrongTypeException expectedNodeFoundInstead(
            String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new ParameterWrongTypeException(gql, String.format("Expected to find a node but found %s instead", got));
    }

    public static ParameterWrongTypeException expectedNodeAtFoundInstead(
            String path, String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new ParameterWrongTypeException(
                gql, String.format("Expected to find a node at '%s' but found %s instead", path, got));
    }

    public static ParameterWrongTypeException expectedEntityAtRefSlotFoundInstead(
            int refSlot, String entity, String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of(entity.toUpperCase()), gotCypherType);
        return new ParameterWrongTypeException(
                gql, String.format("Expected to find a %s at ref slot %s but found %s instead", entity, refSlot, got));
    }

    public static ParameterWrongTypeException expectedEntityAtLongSlotFoundInstead(
            int longSlot, String entity, String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of(entity.toUpperCase()), gotCypherType);
        return new ParameterWrongTypeException(
                gql,
                String.format("Expected to find a %s at long slot %s but found %s instead", entity, longSlot, got));
    }

    public static ParameterWrongTypeException expectedRelFoundInstead(
            String got, String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE"), gotCypherType);
        return new ParameterWrongTypeException(
                gql, String.format("Expected to find a relationship but found %s instead", got));
    }

    public static ParameterWrongTypeException expectedNodeOrRelLabels(String gotPretty, String gotCypherType) {
        var gql = GqlHelper.getGql22G03_22N01(gotPretty, List.of("NODE", "RELATIONSHIP"), gotCypherType);
        return new ParameterWrongTypeException(gql, "Expected a node or relationship when checking types or labels.");
    }

    @Override
    public Status status() {
        return Status.Statement.TypeError;
    }
}
