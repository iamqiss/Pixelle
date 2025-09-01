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
package org.neo4j.shell.completions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.neo4j.shell.parameter.ParameterService;

public abstract class DbInfo implements AutoCloseable {
    ParameterService parameterService;

    public volatile List<String> labels = List.of();

    public volatile List<String> relationshipTypes = List.of();

    public volatile List<String> databaseNames = List.of();

    public volatile List<String> userNames = List.of();

    public volatile List<String> aliasNames = List.of();

    public volatile List<String> roleNames = List.of();

    public volatile List<String> propertyKeys = List.of();

    public volatile List<String> procedures = List.of();

    public volatile List<String> functions = List.of();

    Optional<Boolean> versionCompatibleWithCompletions = Optional.empty();

    public Map<String, CompletionEngine.ParameterType> parameters() {
        Map<String, CompletionEngine.ParameterType> parameters = new HashMap<>();
        parameterService.parameters().forEach((key, value) -> parameters.put(key, parameterType(value)));
        return parameters;
    }

    public DbInfo(ParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void cleanDbInfo() {
        labels = List.of();
        relationshipTypes = List.of();
        databaseNames = List.of();
        userNames = List.of();
        aliasNames = List.of();
        roleNames = List.of();
        propertyKeys = List.of();
        procedures = List.of();
        functions = List.of();
        versionCompatibleWithCompletions = Optional.empty();
    }

    private CompletionEngine.ParameterType parameterType(org.neo4j.driver.Value value) {
        if (value.type().name().equals("STRING")) {
            return CompletionEngine.ParameterType.STRING;
        } else if (value.type().name().equals("MAP")) {
            return CompletionEngine.ParameterType.MAP;
        } else {
            return CompletionEngine.ParameterType.ANY;
        }
    }

    public abstract boolean completionsEnabled();

    public abstract void resumePolling();

    public abstract void stopPolling();
}
