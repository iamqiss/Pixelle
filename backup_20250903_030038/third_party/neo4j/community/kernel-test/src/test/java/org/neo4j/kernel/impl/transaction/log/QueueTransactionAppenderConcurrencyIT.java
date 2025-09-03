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
package org.neo4j.kernel.impl.transaction.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.monitoring.HealthEventGenerator.NO_OP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.util.concurrent.Futures;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
@ExtendWith(RandomExtension.class)
class QueueTransactionAppenderConcurrencyIT {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private RandomSupport random;

    private ThreadPoolJobScheduler jobScheduler;
    private SimpleLogVersionRepository logVersionRepository;
    private SimpleTransactionIdStore transactionIdStore;
    private TransactionMetadataCache metadataCache;
    private DatabaseHealth databaseHealth;
    private NullLogProvider logProvider;
    private ExecutorService executor;
    private SimpleAppendIndexProvider appendIndexProvider;

    @BeforeEach
    void setUp() {
        jobScheduler = new ThreadPoolJobScheduler();

        logVersionRepository = new SimpleLogVersionRepository();
        transactionIdStore = new SimpleTransactionIdStore();
        appendIndexProvider = new SimpleAppendIndexProvider();
        logProvider = NullLogProvider.getInstance();
        metadataCache = new TransactionMetadataCache();
        databaseHealth = new DatabaseHealth(NO_OP, logProvider.getLog(DatabaseHealth.class));
        executor = Executors.newFixedThreadPool(20);
    }

    @AfterEach
    void tearDown() {
        executor.shutdown();
        life.shutdown();
        jobScheduler.close();
    }

    @Test
    void multiThreadedTransactionProcessing() throws IOException, ExecutionException {
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        int numberOfTransactions = 10_000;
        long initialCommittedTxId = transactionIdStore.getLastCommittedTransactionId();

        var results = new ArrayList<Future<?>>(numberOfTransactions);
        for (int i = 0; i < numberOfTransactions; i++) {
            results.add(executor.submit(() -> transactionAppender.append(createTransaction(), LogAppendEvent.NULL)));
        }
        Futures.getAll(results);

        assertEquals(transactionIdStore.getLastCommittedTransactionId(), initialCommittedTxId + numberOfTransactions);
    }

    @Test
    void multiThreadedTransactionWithStop() throws IOException {
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        int numberOfTransactions = 100_000;
        long initialCommittedTxId = transactionIdStore.getLastCommittedTransactionId();
        var results = new ArrayList<Future<?>>(numberOfTransactions);

        int poisonIndex = random.nextInt(numberOfTransactions / 10, numberOfTransactions - 400);
        for (int i = 0; i < numberOfTransactions; i++) {
            results.add(executor.submit(() -> transactionAppender.append(createTransaction(), LogAppendEvent.NULL)));
            if (i == poisonIndex) {
                executor.submit(() -> life.shutdown());
            }
        }

        var futureStatistic = processFutures(results);

        assertEquals(
                transactionIdStore.getLastCommittedTransactionId(),
                initialCommittedTxId + futureStatistic.getCompletedFutures());
        assertThat(futureStatistic.getFailedFutures()).isNotZero();
        assertThat(futureStatistic.getFailedFutures() + futureStatistic.getCompletedFutures())
                .isEqualTo(numberOfTransactions);
    }

    @Test
    void multiThreadedTransactionWithPanic() throws IOException {
        LogFiles logFiles = buildLogFiles(logVersionRepository, transactionIdStore, appendIndexProvider);
        life.add(logFiles);

        QueueTransactionAppender transactionAppender = createAppender(logFiles);
        life.add(transactionAppender);

        int numberOfTransactions = 100_000;
        long initialCommittedTxId = transactionIdStore.getLastCommittedTransactionId();
        var results = new ArrayList<Future<?>>(numberOfTransactions);

        int poisonIndex = random.nextInt(numberOfTransactions / 10, numberOfTransactions - 400);
        for (int i = 0; i < numberOfTransactions; i++) {
            results.add(executor.submit(() -> transactionAppender.append(createTransaction(), LogAppendEvent.NULL)));
            if (i == poisonIndex) {
                executor.submit(
                        () -> databaseHealth.panic(new RuntimeException("Period of intense transaction failures")));
            }
        }

        var futureStatistic = processFutures(results);

        assertEquals(
                transactionIdStore.getLastCommittedTransactionId(),
                initialCommittedTxId + futureStatistic.getCompletedFutures());
        assertThat(futureStatistic.getFailedFutures()).isNotZero();
        assertThat(futureStatistic.getFailedFutures() + futureStatistic.getCompletedFutures())
                .isEqualTo(numberOfTransactions);
    }

    private QueueTransactionAppender createAppender(LogFiles logFiles) {
        TransactionLogQueue logQueue = new TransactionLogQueue(
                logFiles,
                transactionIdStore,
                databaseHealth,
                appendIndexProvider,
                metadataCache,
                jobScheduler,
                logProvider);
        return new QueueTransactionAppender(logQueue);
    }

    private CompleteTransaction createTransaction() {
        CompleteCommandBatch tx = new CompleteCommandBatch(
                List.of(new TestCommand()),
                UNKNOWN_CONSENSUS_INDEX,
                1,
                2,
                3,
                4,
                LatestVersions.LATEST_KERNEL_VERSION,
                ANONYMOUS);
        var transactionCommitment = new TransactionCommitment(transactionIdStore);
        return new CompleteTransaction(
                tx,
                CursorContext.NULL_CONTEXT,
                StoreCursors.NULL,
                transactionCommitment,
                new IdStoreTransactionIdGenerator(transactionIdStore));
    }

    private LogFiles buildLogFiles(
            SimpleLogVersionRepository logVersionRepository,
            SimpleTransactionIdStore transactionIdStore,
            AppendIndexProvider appendIndexProvider)
            throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.builder(databaseLayout, fileSystem, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withLogVersionRepository(logVersionRepository)
                .withRotationThreshold(ByteUnit.mebiBytes(1))
                .withTransactionIdStore(transactionIdStore)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }

    private static FutureStatistic processFutures(List<Future<?>> futures) {
        FutureStatistic statistic = new FutureStatistic();
        for (Future<?> future : futures) {
            try {
                future.get();
                statistic.incrementCompleted();
            } catch (Exception ignored) {
                statistic.incrementFailed();
            }
        }
        return statistic;
    }

    private static final class FutureStatistic {
        int completedFutures;
        int failedFutures;

        void incrementCompleted() {
            completedFutures++;
        }

        void incrementFailed() {
            failedFutures++;
        }

        public int getCompletedFutures() {
            return completedFutures;
        }

        public int getFailedFutures() {
            return failedFutures;
        }
    }
}
