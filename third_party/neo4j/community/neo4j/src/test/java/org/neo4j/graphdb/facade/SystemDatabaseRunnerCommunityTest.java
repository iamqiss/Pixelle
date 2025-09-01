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
package org.neo4j.graphdb.facade;

import java.nio.file.Path;
import java.util.Map;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.api.Neo4jDatabaseManagementServiceBuilder;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;

public class SystemDatabaseRunnerCommunityTest extends SystemDatabaseRunnerAbstractTestBase {
    @Override
    protected Map<Setting<?>, Object> baseConfig() {
        return Map.of();
    }

    @Override
    protected SystemDatabaseRunner.EditionModuleFactory editionModuleFactory() {
        return globalModule -> new CommunityEditionModule(globalModule) {
            @Override
            public void registerDefaultDatabaseInitializer(GlobalModule globalModule) {
                // Don't start default database during system db upgrade
            }
        };
    }

    @Override
    protected Neo4jDatabaseManagementServiceBuilder dbmsBuilder(Path homePath) {
        return new DatabaseManagementServiceBuilder(homePath);
    }
}
