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
package org.neo4j.commandline.dbms;

import static java.lang.Boolean.FALSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.api.Neo4jDatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.SystemGraphComponent.Status;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.SystemDatabaseUpgrader;
import org.neo4j.graphdb.factory.module.edition.migration.MigrationEditionModuleFactory;
import org.neo4j.graphdb.factory.module.edition.migration.SystemDatabaseMigrator;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public abstract class SystemDatabaseUpgraderAbstractTestBase {
    @Inject
    protected Neo4jLayout databaseLayout;

    @Test
    void shouldMigrateSystemDatabase() throws Exception {
        var homeDirectory = databaseLayout.homeDirectory();
        Unzip.unzip(getClass(), previousMajorsSystemDatabase(), homeDirectory);

        var editionFactory = migrationEditionModuleFactory();
        var systemDatabaseMigrator = systemDatabaseMigrator();
        SystemDatabaseUpgrader.upgrade(
                editionFactory,
                systemDatabaseMigrator,
                getConfig(homeDirectory),
                NullLogProvider.getInstance(),
                NullLogProvider.getInstance());

        var dbms = dbmsBuilder(homeDirectory)
                .setConfig(BoltConnector.enabled, FALSE)
                .setConfig(GraphDatabaseInternalSettings.automatic_upgrade_enabled, FALSE)
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .setConfig(baseConfig())
                .build();

        var systemDatabase = dbms.database(SYSTEM_DATABASE_NAME);
        var systemGraphComponents = ((GraphDatabaseAPI) systemDatabase)
                .getDependencyResolver()
                .resolveDependency(SystemGraphComponents.class);

        assertThat(systemGraphComponents.detect(systemDatabase)).isEqualTo(Status.CURRENT);

        dbms.shutdown();
    }

    protected Config getConfig(Path homeDirectory) {
        return Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, homeDirectory)
                .build();
    }

    protected abstract Map<Setting<?>, Object> baseConfig();

    protected abstract MigrationEditionModuleFactory migrationEditionModuleFactory();

    protected abstract SystemDatabaseMigrator systemDatabaseMigrator();

    protected abstract String previousMajorsSystemDatabase();

    protected abstract Neo4jDatabaseManagementServiceBuilder dbmsBuilder(Path homePath);
}
