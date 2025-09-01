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

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.function.Function;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.systemgraph.ContextBasedSystemDatabaseProvider;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

/**
 * Tool used by command line tools in need to start just the system database
 */
public class SystemDatabaseRunner implements Resource {
    public interface EditionModuleFactory extends Function<GlobalModule, AbstractEditionModule> {}

    private final EditionModuleFactory editionFactory;
    private final Config config;
    private final InternalLogProvider systemDbStartupLogProvider;
    private final FileSystemAbstraction fs;

    private GlobalModule globalModule;
    private GraphDatabaseAPI systemDatabase;

    /**
     * Start system db in a minimal context
     *
     * @param fs                         {@link FileSystemAbstraction} file system abstraction to use.
     * @param editionFactory             {@link EditionModuleFactory} factory method for creating edition module.
     * @param config                     {@link Config} configuration.
     * @param systemDbStartupLogProvider {@link InternalLogProvider} where system db startup will be reported.
     */
    public SystemDatabaseRunner(
            FileSystemAbstraction fs,
            EditionModuleFactory editionFactory,
            Config config,
            InternalLogProvider systemDbStartupLogProvider) {
        this.fs = fs;
        this.editionFactory = editionFactory;
        this.config = config;
        this.systemDbStartupLogProvider = systemDbStartupLogProvider;
    }

    public SystemDatabaseRunner run() throws Exception {
        if (globalModule != null) {
            throw new IllegalStateException("System database runner already running");
        }
        var graphDatabaseDependencies = GraphDatabaseDependencies.newDependencies()
                .extensions(Iterables.empty())
                .dependencies(Dependencies.dependenciesOf(fs));
        globalModule = new GlobalModule(config, DbmsInfo.TOOL, false, graphDatabaseDependencies) {
            @Override
            protected LogService createLogService(InternalLogProvider userLogProvider, boolean daemonMode) {
                return new SimpleLogService(systemDbStartupLogProvider);
            }

            @Override
            protected GlobalTransactionEventListeners createGlobalTransactionEventListeners() {
                return GlobalTransactionEventListeners.NULL;
            }
        };
        var globalLife = globalModule.getGlobalLife();

        try {
            var edition = editionFactory.apply(globalModule);
            var globalDependencies = globalModule.getGlobalDependencies();
            globalModule.getGlobalDependencies().satisfyDependency(new GlobalProceduresRegistry());

            var systemGraphComponentsBuilder = new SystemGraphComponents.DefaultBuilder();
            var dbmsRuntimeSystemGraphComponent = new DbmsRuntimeSystemGraphComponent(globalModule.getGlobalConfig());
            systemGraphComponentsBuilder.register(dbmsRuntimeSystemGraphComponent);
            edition.registerSystemGraphComponents(systemGraphComponentsBuilder, globalModule);
            globalDependencies.satisfyDependency(edition.getSystemGraphComponents());

            var databaseContextProvider = edition.createDatabaseContextProvider(globalModule);
            var systemDatabaseProvider = new ContextBasedSystemDatabaseProvider(databaseContextProvider);
            edition.createGlobalReadOnlyChecker(
                    systemDatabaseProvider, databaseContextProvider.databaseIdRepository(), globalModule);
            edition.bootstrapQueryRouterServices(null);
            edition.registerDatabaseInitializers(globalModule, systemDatabaseProvider);

            edition.createDefaultDatabaseResolver(systemDatabaseProvider);
            globalDependencies.satisfyDependency(edition.getDefaultDatabaseResolver());

            edition.createSecurityModule(globalModule);
            SecurityProvider securityProvider = edition.getSecurityProvider();
            globalDependencies.satisfyDependencies(securityProvider.authManager());

            var dbmsVersionProvider = edition.createAndRegisterDbmsRuntimeRepository(
                    globalModule, databaseContextProvider, globalDependencies, dbmsRuntimeSystemGraphComponent);
            globalDependencies.satisfyDependency(dbmsVersionProvider);

            globalLife.start();

            var systemContext = databaseContextProvider
                    .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                    .orElseThrow(() -> new IllegalStateException("Could not start System database"));
            systemDatabase = systemContext.databaseFacade();
        } catch (Exception e) {
            try {
                close();
            } catch (Exception toSuppress) {
                throw Exceptions.chain(e, toSuppress);
            }
            throw e;
        }
        return this;
    }

    @Override
    public void close() {
        if (globalModule != null) {
            globalModule.getGlobalLife().shutdown();
            globalModule = null;
        }
    }

    public GlobalModule globalModule() {
        return globalModule;
    }

    public GraphDatabaseAPI systemDatabase() {
        return systemDatabase;
    }
}
