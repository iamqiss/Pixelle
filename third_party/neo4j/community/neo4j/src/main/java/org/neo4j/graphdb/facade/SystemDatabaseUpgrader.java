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

import java.util.concurrent.TimeUnit;
import org.neo4j.commandline.dbms.MigrateStoreCommand;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.edition.migration.MigrationEditionModuleFactory;
import org.neo4j.graphdb.factory.module.edition.migration.SystemDatabaseMigrator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.storemigration.VisibleMigrationProgressMonitorFactory;
import org.neo4j.logging.InternalLogProvider;

/**
 * Tool used by {@link MigrateStoreCommand} to upgrade system db.
 */
public class SystemDatabaseUpgrader {
    private SystemDatabaseUpgrader() {}

    /**
     * Start System db in a minimal context and upgrade:
     * <ol>
     * <li>Upgrade all system graph components</li>
     * <li>Wait for indexes to come online.</li>
     * </ol>
     * @param editionFactory             {@link MigrationEditionModuleFactory} factory method for creating edition module.
     * @param systemDatabaseMigrator     {@link SystemDatabaseMigrator} edition specific additional steps during migration.
     * @param config                     {@link Config} configuration.
     * @param logProvider                {@link InternalLogProvider} where progress will be reported.
     * @param systemDbStartupLogProvider {@link InternalLogProvider} where system db startup will be reported.
     */
    public static void upgrade(
            MigrationEditionModuleFactory editionFactory,
            SystemDatabaseMigrator systemDatabaseMigrator,
            Config config,
            InternalLogProvider logProvider,
            InternalLogProvider systemDbStartupLogProvider)
            throws Exception {
        var log = logProvider.getLog(SystemDatabaseUpgrader.class);
        var progressMonitor = VisibleMigrationProgressMonitorFactory.forSystemUpgrade(log);
        progressMonitor.started(3);
        var bootstrapProgress = progressMonitor.startSection("Bootstrap");
        try (var systemDatabaseRunner = new SystemDatabaseRunner(
                        new DefaultFileSystemAbstraction(), editionFactory, config, systemDbStartupLogProvider)
                .run()) {
            bootstrapProgress.close();

            var systemDatabase = systemDatabaseRunner.systemDatabase();

            // Wait for indexes to come online
            var indexPopulationProgress = progressMonitor.startSection("Index population");
            try (var tx = systemDatabase.beginTx()) {
                tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
            }
            indexPopulationProgress.close();

            // Remove topology graph object which are not needed
            var purgeTopologyGraphProgress = progressMonitor.startSection("Purge topology graph");
            try (var tx = systemDatabase.beginTx()) {
                systemDatabaseMigrator.performAdditionalSystemDatabaseMigrationSteps(
                        systemDatabase, tx, systemDatabaseRunner.globalModule().getGlobalClock(), log);
                tx.commit();
            }
            purgeTopologyGraphProgress.close();
        }
        progressMonitor.completed();
    }
}
