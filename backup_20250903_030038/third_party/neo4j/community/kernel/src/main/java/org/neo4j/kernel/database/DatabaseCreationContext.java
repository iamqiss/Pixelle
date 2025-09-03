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
package org.neo4j.kernel.database;

import java.util.function.Function;
import java.util.function.LongFunction;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.device.DeviceMapper;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.prefetch.PagePrefetcher;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.api.CommandCommitListeners;
import org.neo4j.kernel.impl.api.ExternalIdReuseConditionProvider;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.api.TransactionalProcessFactory;
import org.neo4j.kernel.impl.api.TransactionsFactory;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.index.DatabaseIndexStats;
import org.neo4j.kernel.impl.pagecache.IOControllerService;
import org.neo4j.kernel.impl.pagecache.VersionStorageFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.locker.FileLockerService;
import org.neo4j.kernel.monitoring.DatabaseEventListeners;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.TokenHolders;

public interface DatabaseCreationContext {
    ServerIdentity getServerIdentity();

    NamedDatabaseId getNamedDatabaseId();

    DatabaseLayout getDatabaseLayout();

    DatabaseConfig getDatabaseConfig();

    IdGeneratorFactory getIdGeneratorFactory();

    DatabaseLogService getDatabaseLogService();

    JobScheduler getScheduler();

    DependencyResolver getGlobalDependencies();

    TokenHolders getTokenHolders();

    GlobalTransactionEventListeners getTransactionEventListeners();

    FileSystemAbstraction getFs();

    DatabaseTransactionStats getTransactionStats();

    DatabaseIndexStats getIndexStats();

    Factory<DatabaseHealth> getDatabaseHealthFactory();

    TransactionalProcessFactory getCommitProcessFactory();

    PageCache getPageCache();

    ConstraintSemantics getConstraintSemantics();

    Monitors getMonitors();

    DatabaseTracers getTracers();

    GlobalProcedures getGlobalProcedures();

    IOControllerService getIoControllerService();

    LongFunction<DatabaseAvailabilityGuard> getDatabaseAvailabilityGuardFactory();

    SystemNanoClock getClock();

    StoreCopyCheckPointMutex getStoreCopyCheckPointMutex();

    IdController getIdController();

    DbmsInfo getDbmsInfo();

    HostedOnMode getMode();

    CollectionsFactorySupplier getCollectionsFactorySupplier();

    Iterable<ExtensionFactory<?>> getExtensionFactories();

    Function<DatabaseLayout, DatabaseLayoutWatcher> getWatcherServiceFactory();

    QueryEngineProvider getEngineProvider();

    DatabaseEventListeners getDatabaseEventListeners();

    StorageEngineFactorySupplier getStorageEngineFactorySupplier();

    FileLockerService getFileLockerService();

    AccessCapabilityFactory getAccessCapabilityFactory();

    LeaseService getLeaseService();

    DatabaseStartupController getStartupController();

    GlobalMemoryGroupTracker getTransactionsMemoryPool();

    GlobalMemoryGroupTracker getOtherMemoryPool();

    ReadOnlyDatabases getDbmsReadOnlyChecker();

    CursorContextFactory getContextFactory();

    ExternalIdReuseConditionProvider externalIdReuseConditionProvider();

    static StorageEngineFactory selectStorageEngine(
            FileSystemAbstraction fs, Neo4jLayout neo4jLayout, Configuration config, NamedDatabaseId namedDatabaseId) {
        return StorageEngineFactory.selectStorageEngine(fs, neo4jLayout.databaseLayout(namedDatabaseId.name()), config);
    }

    VersionStorageFactory getVersionStorageFactory();

    DeviceMapper getDeviceMapper();

    TransactionsFactory getTransactionsFactory();

    CommandCommitListeners getCommandCommitListeners();

    PagePrefetcher getPagePrefetcher();
}
