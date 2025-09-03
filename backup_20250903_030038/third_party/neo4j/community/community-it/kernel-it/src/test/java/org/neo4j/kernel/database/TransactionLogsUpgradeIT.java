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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.KernelVersion.GLORIOUS_FUTURE;
import static org.neo4j.kernel.TxLogValidationUtils.assertLogHeaderExpectedVersion;
import static org.neo4j.kernel.TxLogValidationUtils.assertWholeTransactionsIn;
import static org.neo4j.kernel.TxLogValidationUtils.assertWholeTransactionsWithCorrectVersionInSpecificLogVersion;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.DEFAULT_LOG_SEGMENT_SIZE;
import static org.neo4j.kernel.recovery.RecoveryHelpers.getLatestCheckpoint;
import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.UpgradeTestUtil.assertKernelVersion;
import static org.neo4j.test.UpgradeTestUtil.createWriteTransaction;
import static org.neo4j.test.UpgradeTestUtil.upgradeDatabase;
import static org.neo4j.test.UpgradeTestUtil.upgradeDbms;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.tracer.DefaultDatabaseTracer;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.kernel.recovery.RecoveryHelpers;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Barrier;
import org.neo4j.test.Race;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
class TransactionLogsUpgradeIT {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private Neo4jLayout neo4jLayout;

    private DatabaseManagementService managementService;
    private GraphDatabaseAPI testDb;
    private CommandReaderFactory commandReaderFactory;

    @BeforeEach
    void setUp() {
        startDbms(builder ->
                builder.setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, DEFAULT_LOG_SEGMENT_SIZE * 3L));

        commandReaderFactory = ((GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME))
                .getDependencyResolver()
                .resolveDependency(StorageEngineFactory.class)
                .commandReaderFactory();
    }

    @AfterEach
    void tearDown() {
        shutdownDbms();
    }

    private TestDatabaseManagementServiceBuilder configureGloriousFutureAsLatest(
            TestDatabaseManagementServiceBuilder builder) {
        return builder.setConfig(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())
                .setConfig(GraphDatabaseInternalSettings.latest_kernel_version, GLORIOUS_FUTURE.version());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRotateOnKernelVersionChangeAndGetCorrectInfoInLogHeader(boolean useQueueAppender) throws Exception {
        shutdownDbms();
        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));

        createWriteTransaction(testDb);
        assertKernelVersion(testDb, LATEST_KERNEL_VERSION);

        upgradeDatabase(managementService, testDb, LATEST_KERNEL_VERSION, GLORIOUS_FUTURE);

        long firstNewTransaction = testDb.getDependencyResolver()
                .resolveDependency(TransactionIdStore.class)
                .getLastClosedTransactionId();
        LogFiles logFiles = testDb.getDependencyResolver().resolveDependency(LogFiles.class);
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION,
                null /* latest format doesn't include the version */,
                TransactionIdStore.BASE_TX_ID);
        AtomicInteger latestChecksum = new AtomicInteger();
        assertWholeTransactionsIn(
                logFiles.getLogFile(),
                INITIAL_LOG_VERSION,
                (startEntry) -> {},
                (commitEntry) -> latestChecksum.set(commitEntry.getChecksum()),
                commandReaderFactory);
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                logFiles.getLogFile().getHighestLogVersion(),
                GLORIOUS_FUTURE,
                firstNewTransaction - 1 /* should point at the upgrade tx */,
                latestChecksum.get());

        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(),
                INITIAL_LOG_VERSION,
                LATEST_KERNEL_VERSION,
                (int) (firstNewTransaction - 1 - TransactionIdStore.BASE_TX_ID),
                commandReaderFactory);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(), INITIAL_LOG_VERSION + 1, GLORIOUS_FUTURE, 1, commandReaderFactory);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void canFindNextLogFileIfHaveFileWithJustHeader(boolean useQueueAppender) throws Throwable {
        // There is a corner case where the upgrade transaction can trigger a rotation just after being written. And
        // then the transaction after the upgrade also triggers a rotation because it is on a new version. There will
        // then be an empty file in the middle, but it should still work.
        var defaultDatabaseTracer = testDb.getDependencyResolver().resolveDependency(DefaultDatabaseTracer.class);

        // Fill log with slightly more than we need to trigger rotation later
        while (defaultDatabaseTracer.appendedBytes() < DEFAULT_LOG_SEGMENT_SIZE * 2.2) {
            createWriteTransaction(testDb);
        }
        assertThat(defaultDatabaseTracer.numberOfLogRotations()).isEqualTo(0);
        long nodeCountBeforeTxTriggeringUpgrade = getNodeCount(testDb);
        long lastClosedTransactionIdBeforeUpgrade = testDb.getDependencyResolver()
                .resolveDependency(TransactionIdStore.class)
                .getLastClosedTransactionId();

        shutdownDbms();
        // Set rotation so that the first transaction (upgrade) should trigger rotation.
        // We should then end up with one log file with everything including upgrade tx,
        // one log file that only contains a header and one logfile with the tx in the new version.
        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, DEFAULT_LOG_SEGMENT_SIZE * 2L)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));
        upgradeDatabase(managementService, testDb, LATEST_KERNEL_VERSION, GLORIOUS_FUTURE);

        DatabaseLayout dbLayout = testDb.databaseLayout();
        var tracer = ((GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME))
                .getDependencyResolver()
                .resolveDependency(DefaultDatabaseTracer.class);
        assertThat(tracer.numberOfLogRotations()).isEqualTo(2);

        shutdownDbms();

        var config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.latest_kernel_version, GLORIOUS_FUTURE.version())
                .build();
        RecoveryHelpers.removeLastCheckpointRecordFromLogFile(dbLayout, fileSystem, config);
        assertThat(getLatestCheckpoint(dbLayout, fileSystem, config).kernelVersion())
                .isEqualTo(LATEST_KERNEL_VERSION);

        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));
        assertKernelVersion(testDb, GLORIOUS_FUTURE);
        // We managed to read all the way passed the empty log file and saw the tx in the last logfile.
        assertThat(getNodeCount(testDb)).isEqualTo(nodeCountBeforeTxTriggeringUpgrade + 1);

        LogFiles logFiles = testDb.getDependencyResolver().resolveDependency(LogFiles.class);
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION,
                null /* latest format doesn't contain version yet */,
                TransactionIdStore.BASE_TX_ID);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(),
                INITIAL_LOG_VERSION,
                LATEST_KERNEL_VERSION,
                (int) (lastClosedTransactionIdBeforeUpgrade + 1 - TransactionIdStore.BASE_TX_ID),
                commandReaderFactory);
        assertLogHeaderExpectedVersion(
                fileSystem, logFiles, INITIAL_LOG_VERSION + 1, null, lastClosedTransactionIdBeforeUpgrade + 1);
        assertThat(fileSystem.getFileSize(logFiles.getLogFile().getLogFileForVersion(INITIAL_LOG_VERSION + 1)))
                .isEqualTo(LogFormat.fromKernelVersion(LATEST_KERNEL_VERSION).getHeaderSize());
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(), INITIAL_LOG_VERSION + 1, LATEST_KERNEL_VERSION, 0, commandReaderFactory);
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION + 2,
                GLORIOUS_FUTURE,
                lastClosedTransactionIdBeforeUpgrade + 1);
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(), INITIAL_LOG_VERSION + 2, GLORIOUS_FUTURE, 1, commandReaderFactory);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldRotateToNewFileWhenUpgradeTxIsLastOnStartup(boolean useQueueAppender) throws Exception {
        shutdownDbms();
        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));

        TransactionIdStore transactionIdStore =
                testDb.getDependencyResolver().resolveDependency(TransactionIdStore.class);
        long lastClosedTransactionIdBeforeUpgrade = transactionIdStore.getLastClosedTransactionId();

        long numNodesBefore = getNodeCount(testDb);

        // Register a handler that will make the transaction triggering the upgrade fail
        managementService.registerTransactionEventListener(
                DEFAULT_DATABASE_NAME, new InternalTransactionEventListener.Adapter<>() {
                    @Override
                    public Object beforeCommit(
                            TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                        if (data.metaData().containsKey("triggerTx")) {
                            throw new TransactionFailureException(
                                    "Failed because you asked for it", Status.Transaction.TransactionHookFailed);
                        }
                        return null;
                    }
                });

        // then upgrade dbms runtime to trigger db upgrade on next write
        upgradeDbms(managementService);

        GraphDatabaseAPI finalTestDb = testDb;
        assertThatThrownBy(() -> {
                    try (TransactionImpl tx = (TransactionImpl) finalTestDb.beginTx()) {
                        // metadata indicating we want it to fail
                        tx.setMetaData(Map.of("triggerTx", "something"));
                        tx.createNode(); // and make sure it is a write to trigger upgrade
                        tx.commit();
                    }
                })
                .isInstanceOf(TransactionFailureException.class);

        assertThat(getNodeCount(testDb))
                .as("Triggering transaction succeeded when it should fail")
                .isEqualTo(numNodesBefore);
        assertKernelVersion(testDb, GLORIOUS_FUTURE);
        LogPosition positionAfterUpgrade =
                transactionIdStore.getLastClosedTransaction().logPosition();

        // Now the upgrade transaction is our latest transaction in the log, and it is on the 'old' version
        shutdownDbms();

        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));
        assertKernelVersion(testDb, GLORIOUS_FUTURE);

        createWriteTransaction(testDb);

        LogFiles logFiles = testDb.getDependencyResolver().resolveDependency(LogFiles.class);

        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION,
                null /* latest format doesn't contain version yet */,
                TransactionIdStore.BASE_TX_ID);
        AtomicInteger latestChecksum = new AtomicInteger();
        int nbrTxs = assertWholeTransactionsIn(
                logFiles.getLogFile(),
                INITIAL_LOG_VERSION,
                (startEntry) -> assertThat(startEntry.kernelVersion()).isEqualTo(LATEST_KERNEL_VERSION),
                (commitEntry) -> latestChecksum.set(commitEntry.getChecksum()),
                commandReaderFactory);
        assertThat(nbrTxs).isEqualTo((int) (lastClosedTransactionIdBeforeUpgrade - TransactionIdStore.BASE_TX_ID + 1));
        assertThat(fileSystem.getFileSize(logFiles.getLogFile().getLogFileForVersion(INITIAL_LOG_VERSION)))
                .isEqualTo(positionAfterUpgrade.getByteOffset());
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION + 1,
                GLORIOUS_FUTURE,
                lastClosedTransactionIdBeforeUpgrade + 1,
                latestChecksum.get());
        assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(), INITIAL_LOG_VERSION + 1, GLORIOUS_FUTURE, 1, commandReaderFactory);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void makeCleanRotationOnVersionChange(boolean useQueueAppender) throws Throwable {
        int nbrTxsPerContestant = 10;
        int nbrContestants = 5;

        shutdownDbms();
        startDbms(builder -> configureGloriousFutureAsLatest(builder)
                .setConfig(GraphDatabaseInternalSettings.dedicated_transaction_appender, useQueueAppender));

        long txsBefore = testDb.getDependencyResolver()
                        .resolveDependency(TransactionIdStore.class)
                        .getLastCommittedTransactionId()
                - TransactionIdStore.BASE_TX_ID;

        Barrier.Control atLeastOneTxDoneBeforeAfter = new Barrier.Control();
        Race race = new Race();
        race.addContestant(
                () -> {
                    createWriteTransaction(testDb);
                    atLeastOneTxDoneBeforeAfter.reached();
                    createWriteTransaction(testDb);
                },
                1);
        race.addContestants(
                nbrContestants,
                () -> {
                    for (int i = 0; i < nbrTxsPerContestant; i++) {
                        createWriteTransaction(testDb);
                    }
                },
                1);
        race.addContestant(
                Race.throwing(() -> {
                    atLeastOneTxDoneBeforeAfter.await();
                    upgradeDbms(managementService);
                    atLeastOneTxDoneBeforeAfter.release();
                }),
                1);

        race.go();

        LogFiles logFiles = testDb.getDependencyResolver().resolveDependency(LogFiles.class);
        assertLogHeaderExpectedVersion(
                fileSystem,
                logFiles,
                INITIAL_LOG_VERSION,
                null /* latest format doesn't include the version */,
                TransactionIdStore.BASE_TX_ID);
        assertLogHeaderExpectedVersion(
                fileSystem, logFiles, logFiles.getLogFile().getHighestLogVersion(), GLORIOUS_FUTURE);

        int nbrTxsIn0 = assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(), INITIAL_LOG_VERSION, LATEST_KERNEL_VERSION, commandReaderFactory);
        assertThat(nbrTxsIn0).isGreaterThanOrEqualTo(2); // At least upgrade and one before
        int nbrTxsIn1 = assertWholeTransactionsWithCorrectVersionInSpecificLogVersion(
                logFiles.getLogFile(), INITIAL_LOG_VERSION + 1, GLORIOUS_FUTURE, commandReaderFactory);
        assertThat(nbrTxsIn1).isGreaterThanOrEqualTo(1); // At least the one waiting for the barrier
        assertThat(nbrTxsIn0 + nbrTxsIn1)
                .isEqualTo(nbrContestants * nbrTxsPerContestant
                        + 2 /* the guaranteed before and after */
                        + 1 /* the version update */
                        + txsBefore);
    }

    @Test
    void shouldReadOverFormatSwitch() throws Exception {
        shutdownDbms();
        startDbms(this::configureGloriousFutureAsLatest);

        createWriteTransaction(testDb);
        assertKernelVersion(testDb, LATEST_KERNEL_VERSION);

        upgradeDatabase(managementService, testDb, LATEST_KERNEL_VERSION, GLORIOUS_FUTURE);
        createWriteTransaction(testDb);

        LogFiles logFiles = testDb.getDependencyResolver().resolveDependency(LogFiles.class);

        assertThat(assertWholeTransactionsIn(
                        logFiles.getLogFile(),
                        INITIAL_LOG_VERSION,
                        (startEntry) -> {},
                        (commitEntry) -> {},
                        commandReaderFactory,
                        ReaderLogVersionBridge.forFile(logFiles.getLogFile())))
                .isBetween(5, 6); // One extra token tx on record
    }

    private long getNodeCount(GraphDatabaseAPI db) {
        try (Transaction tx = db.beginTx()) {
            return Iterables.count(tx.getAllNodes());
        }
    }

    private void startDbms(Configuration configuration) {
        managementService = configuration
                .configure(new TestDatabaseManagementServiceBuilder(neo4jLayout)
                        .setConfig(GraphDatabaseSettings.keep_logical_logs, "keep_all")
                        .setConfig(automatic_upgrade_enabled, false))
                .build();
        testDb = (GraphDatabaseAPI) managementService.database(DEFAULT_DATABASE_NAME);
    }

    private void shutdownDbms() {
        if (managementService != null) {
            managementService.shutdown();
            managementService = null;
        }
    }

    @FunctionalInterface
    interface Configuration {
        TestDatabaseManagementServiceBuilder configure(TestDatabaseManagementServiceBuilder builder);
    }
}
