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
package org.neo4j.dbms.api;

import static java.lang.String.format;

import java.util.List;
import org.neo4j.gqlstatus.GqlHelper;

/**
 * This helper class contains methods to create `DatabaseNotFoundException`. These would normally be on the exception
 * class itself, but that is `@PublicApi`, and we don't want these methods to be public API.
 */
public class DatabaseNotFoundHelper {

    public static DatabaseNotFoundException databaseNotFound(String databaseName) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new DatabaseNotFoundException(gql, "database not found: " + databaseName);
    }

    public static DatabaseNotFoundException cannotFindDatabase(String databaseName) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new DatabaseNotFoundException(gql, "Cannot find database: " + databaseName);
    }

    public static DatabaseNotFoundException databaseDoesNotExist(String databaseName) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new DatabaseNotFoundException(gql, "Database does not exist: " + databaseName);
    }

    public static DatabaseNotFoundException databaseWithNameNotFound(String databaseName) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new DatabaseNotFoundException(gql, format("Database with name `%s` not found.", databaseName));
    }

    public static DatabaseNotFoundException noDatabaseFoundWithNameAlias(String databaseName) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new DatabaseNotFoundException(
                gql, String.format("No database found with name/alias '%s'", databaseName));
    }

    public static DatabaseNotFoundException noDatabaseFoundWithNameAliasOnServers(
            String databaseName, List<String> servers) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new DatabaseNotFoundException(
                gql, String.format("No database found with name/alias '%s' on server(s) '%s'", databaseName, servers));
    }

    public static DatabaseNotFoundException defaultDatabaseNotFound(String defaultDatabaseName) {
        var gql = GqlHelper.getGql22000_22N51(defaultDatabaseName);
        return new DatabaseNotFoundException(gql, "Default database not found: " + defaultDatabaseName);
    }

    public static DatabaseNotFoundException failedToRecreateDatabaseNotFound(String databaseName) {
        var gql = GqlHelper.getGql22000_22N51(databaseName);
        return new DatabaseNotFoundException(
                gql,
                format(
                        "Failed to recreate the specified database '%s': No database exists with that name or alias.",
                        databaseName));
    }

    public static DatabaseNotFoundException failedCreateCompositeAlias(String name, String nonPrettyName) {
        var gql = GqlHelper.getGql42002_42N00(name);
        return new DatabaseNotFoundException(
                gql,
                format(
                        "Failed to create the specified database alias '%s': "
                                + "Composite database '%s' does not exist.",
                        name, nonPrettyName));
    }

    public static DatabaseNotFoundException failedDeleteComposite(String name) {
        var gql = GqlHelper.getGql42002_42N00(name);
        return new DatabaseNotFoundException(
                gql, format("Failed to delete the specified composite database '%s': Database does not exist.", name));
    }

    public static DatabaseNotFoundException failedAction(String action, String name) {
        var gql = GqlHelper.getGql42002_42N00(name);
        return new DatabaseNotFoundException(
                gql, format("Failed to %s the specified database '%s': Database does not exist.", action, name));
    }

    public static DatabaseNotFoundException failedActionAlias(String action, String alias, String name) {
        var gql = GqlHelper.getGql42002_42N00(name);
        return new DatabaseNotFoundException(
                gql,
                format(
                        "Failed to %s the specified database alias '%s': Database '%s' does not exist.",
                        action, alias, name));
    }

    public static DatabaseNotFoundException noNameOrAlias(String name) {
        var gql = GqlHelper.getGql42002_42N00(name);
        return new DatabaseNotFoundException(
                gql, format("Database '%s' does not exist': No database exists with that name or alias.", name));
    }
}
