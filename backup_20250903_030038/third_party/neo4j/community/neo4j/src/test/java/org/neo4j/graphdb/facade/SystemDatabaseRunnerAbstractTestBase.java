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

import static java.lang.Boolean.FALSE;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.api.Neo4jDatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.utils.TestDirectory;

@Neo4jLayoutExtension
public abstract class SystemDatabaseRunnerAbstractTestBase {
    @Inject
    protected Neo4jLayout neoLayout;

    @Inject
    protected TestDirectory testDirectory;

    @Test
    void shouldOnlyStartSystemDb() throws Exception {
        createDatabase();

        var editionFactory = editionModuleFactory();
        try (var systemDatabaseRunner = new SystemDatabaseRunner(
                testDirectory.getFileSystem(),
                editionFactory,
                getConfig(neoLayout.homeDirectory()),
                NullLogProvider.getInstance())) {
            assertThat(systemDatabaseRunner.globalModule()).isNull();
            assertThat(systemDatabaseRunner.systemDatabase()).isNull();

            systemDatabaseRunner.run();

            assertThat(systemDatabaseRunner.globalModule()).isNotNull();
            assertThat(systemDatabaseRunner.systemDatabase()).isNotNull();

            var databaseContextProvider = (DatabaseContextProvider<? extends DatabaseContext>) systemDatabaseRunner
                    .globalModule()
                    .getGlobalDependencies()
                    .resolveDependency(DatabaseContextProvider.class);
            assertThat(databaseContextProvider.registeredDatabases()).hasSize(1);

            var systemDatabaseContext = databaseContextProvider.getDatabaseContext(DatabaseId.SYSTEM_DATABASE_ID);
            assertThat(systemDatabaseContext).isNotEmpty();
            assertThat(systemDatabaseContext.map(DatabaseContext::databaseFacade))
                    .hasValue(systemDatabaseRunner.systemDatabase());
        }
    }

    protected Config getConfig(Path homeDirectory) {
        return Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, homeDirectory)
                .build();
    }

    private void createDatabase() {
        var dbms = dbmsBuilder(neoLayout.homeDirectory())
                .setConfig(BoltConnector.enabled, FALSE)
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .setConfig(baseConfig())
                .build();
        dbms.shutdown();
    }

    protected abstract Map<Setting<?>, Object> baseConfig();

    protected abstract SystemDatabaseRunner.EditionModuleFactory editionModuleFactory();

    protected abstract Neo4jDatabaseManagementServiceBuilder dbmsBuilder(Path homePath);
}
