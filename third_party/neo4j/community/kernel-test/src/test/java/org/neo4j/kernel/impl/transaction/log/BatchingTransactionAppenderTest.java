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

import static java.util.Collections.emptyIterator;
import static org.apache.commons.io.IOUtils.EMPTY_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.latest_kernel_version;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.latest_runtime_version;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.KernelVersion.DEFAULT_BOOTSTRAP_VERSION;
import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED;
import static org.neo4j.kernel.impl.transaction.log.LogChannelUtils.getReadChannel;
import static org.neo4j.kernel.impl.transaction.log.LogChannelUtils.getWriteChannel;
import static org.neo4j.kernel.impl.transaction.log.LogIndexEncoding.encodeLogIndex;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newCommitEntry;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryFactory.newStartEntry;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.Commitment.NO_COMMITMENT;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.BINARY_VERSIONS;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.mockito.Mockito;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.api.CommandCommitListeners;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.txid.IdStoreTransactionIdGenerator;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.CompleteBatchRepresentation;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Panic;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.arguments.KernelVersionSource;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(LifeExtension.class)
class BatchingTransactionAppenderTest {
    private final Panic databasePanic = mock(DatabaseHealth.class);
    private final LogFile logFile = mock(LogFile.class);
    private final LogFiles logFiles = mock(TransactionLogFiles.class);
    private final TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
    private final TransactionMetadataCache positionCache = new TransactionMetadataCache();
    private final TransactionIdGenerator transactionIdGenerator = new IdStoreTransactionIdGenerator(transactionIdStore);

    @Inject
    private LifeSupport life;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private FileSystemAbstraction fs;

    private Path path;

    @BeforeEach
    void setUp() {
        path = testDirectory.file("transactions");
        when(logFiles.getLogFile()).thenReturn(logFile);
        when(transactionIdStore.getLastCommittedTransaction())
                .thenReturn(new TransactionId(
                        BASE_TX_ID, BASE_APPEND_INDEX, DEFAULT_BOOTSTRAP_VERSION, BASE_TX_CHECKSUM, 1, 2));
    }

    @Test
    void shouldBeAbleToAppendTransactionWithoutKernelVersion()
            throws IOException, ExecutionException, InterruptedException {
        CountingVersionProvider versionProvider = new CountingVersionProvider();
        try (var writeChannel = getWriteChannel(fs, path, LATEST_KERNEL_VERSION)) {
            when(logFile.getTransactionLogWriter())
                    .thenReturn(new TransactionLogWriter(
                            writeChannel, versionProvider, BINARY_VERSIONS, LogRotation.NO_ROTATION));

            long txId = 15;
            when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);
            when(transactionIdStore.getLastCommittedTransaction())
                    .thenReturn(new TransactionId(
                            txId,
                            txId + 2,
                            DEFAULT_BOOTSTRAP_VERSION,
                            BASE_TX_CHECKSUM,
                            BASE_TX_COMMIT_TIMESTAMP,
                            UNKNOWN_CONSENSUS_INDEX));
            TransactionAppender appender = life.add(createTransactionAppender());
            CommandBatch transaction =
                    new CompleteCommandBatch(singleTestCommand(), 5, 12345, 7896, 123456, -1, null, ANONYMOUS);

            // Looked up to figure out previous version on creation of writer
            assertEquals(1, versionProvider.getVersionLookedUp());

            appender.append(
                    new CompleteTransaction(
                            transaction, NULL_CONTEXT, StoreCursors.NULL, NO_COMMITMENT, TransactionIdGenerator.EMPTY),
                    LogAppendEvent.NULL);

            assertEquals(2, versionProvider.getVersionLookedUp());
        }
        final LogEntryReader logEntryReader = logEntryReader();
        try (var readChannel = getReadChannel(fs, path, LATEST_KERNEL_VERSION);
                var reader = new CommittedCommandBatchCursor(readChannel, logEntryReader)) {
            reader.next();
            CommittedCommandBatchRepresentation commandBatch = reader.get();
            assertEquals(LATEST_KERNEL_VERSION, commandBatch.commandBatch().kernelVersion());
        }
    }

    @ParameterizedTest
    @KernelVersionSource(atLeast = "5.0")
    void shouldAppendSingleTransaction(KernelVersion kernelVersion) throws Exception {
        assertShouldAppendSingleTransaction(kernelVersion, BINARY_VERSIONS);
    }

    @Test
    void shouldAppendSingleTransactionInGloriousFuture() throws Exception {
        assertShouldAppendSingleTransaction(
                KernelVersion.GLORIOUS_FUTURE,
                new BinarySupportedKernelVersions(Config.newBuilder()
                        .set(latest_kernel_version, KernelVersion.GLORIOUS_FUTURE.version())
                        .set(latest_runtime_version, DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())
                        .build()));
    }

    @Test
    void shouldAppendBatchOfTransactions() throws Exception {
        try (var writeChannel = getWriteChannel(fs, path, LATEST_KERNEL_VERSION)) {
            TransactionLogWriter logWriter = new TransactionLogWriter(
                    writeChannel, LATEST_KERNEL_VERSION_PROVIDER, BINARY_VERSIONS, LogRotation.NO_ROTATION);
            TransactionLogWriter logWriterSpy = spy(logWriter);
            when(logFile.getTransactionLogWriter()).thenReturn(logWriterSpy);

            TransactionAppender appender = life.add(createTransactionAppender());
            when(transactionIdStore.nextCommittingTransactionId()).thenReturn(2L, 3L, 4L);
            CommandBatch batch1 = transaction(singleTestCommand(), 0, 0, 1, 0, LATEST_KERNEL_VERSION);
            CommandBatch batch2 = transaction(singleTestCommand(), 0, 0, 1, 0, LATEST_KERNEL_VERSION);
            CommandBatch batch3 = transaction(singleTestCommand(), 0, 0, 1, 0, LATEST_KERNEL_VERSION);
            CompleteTransaction batch = batchOf(batch1, batch2, batch3);
            appender.append(batch, LogAppendEvent.NULL);

            verify(logWriterSpy).append(eq(batch1), eq(2L), anyLong(), anyLong(), anyInt(), anyLong(), any());
            verify(logWriterSpy).append(eq(batch2), eq(3L), anyLong(), anyLong(), anyInt(), anyLong(), any());
            verify(logWriterSpy).append(eq(batch3), eq(4L), anyLong(), anyLong(), anyInt(), anyLong(), any());
        }
    }

    @Test
    void shouldAppendCommittedTransactions() throws Exception {
        final long nextTxId = 15;
        final byte[] additionalHeader = encodeLogIndex(5);
        final long timeStarted = 12345;
        final long latestCommittedTxWhenStarted = nextTxId - 5;
        final long timeCommitted = timeStarted + 10;
        LogEntryStart start = newStartEntry(
                LATEST_KERNEL_VERSION,
                timeStarted,
                latestCommittedTxWhenStarted,
                latestCommittedTxWhenStarted + 7,
                0,
                additionalHeader,
                LogPosition.UNSPECIFIED);
        LogEntryCommit commit = newCommitEntry(LATEST_KERNEL_VERSION, nextTxId, timeCommitted, BASE_TX_CHECKSUM);
        CompleteBatchRepresentation transaction = new CompleteBatchRepresentation(start, singleTestCommand(), commit);

        try (var writeChannel = getWriteChannel(fs, path, LATEST_KERNEL_VERSION)) {
            doReturn(new TransactionLogWriter(
                            writeChannel, LATEST_KERNEL_VERSION_PROVIDER, BINARY_VERSIONS, LogRotation.NO_ROTATION))
                    .when(logFile)
                    .getTransactionLogWriter();

            doReturn(nextTxId).when(transactionIdStore).nextCommittingTransactionId();
            doReturn(new TransactionId(
                            nextTxId,
                            nextTxId + 2,
                            DEFAULT_BOOTSTRAP_VERSION,
                            BASE_TX_CHECKSUM,
                            BASE_TX_COMMIT_TIMESTAMP,
                            UNKNOWN_CONSENSUS_INDEX))
                    .when(transactionIdStore)
                    .getLastCommittedTransaction();
            TransactionAppender appender = life.add(new BatchingTransactionAppender(
                    logFiles, transactionIdStore, databasePanic, new SimpleAppendIndexProvider(), positionCache));

            appender.append(
                    new CompleteTransaction(
                            transaction,
                            NULL_CONTEXT,
                            StoreCursors.NULL,
                            new TransactionCommitment(transactionIdStore),
                            transactionIdGenerator),
                    LogAppendEvent.NULL);
        }

        LogEntryReader logEntryReader = logEntryReader();
        try (var readChannel = getReadChannel(fs, path, LATEST_KERNEL_VERSION);
                CommittedCommandBatchCursor reader = new CommittedCommandBatchCursor(readChannel, logEntryReader)) {
            reader.next();
            CommittedCommandBatchRepresentation commandBatch = reader.get();
            CommandBatch result = commandBatch.commandBatch();
            assertEquals(5, result.consensusIndex());
            assertEquals(timeStarted, result.getTimeStarted());
            assertEquals(timeCommitted, commandBatch.timeWritten());
            assertEquals(latestCommittedTxWhenStarted, result.getLatestCommittedTxWhenStarted());
        }
    }

    @Test
    void shouldNotAppendCommittedTransactionsWhenTooFarAhead() {
        // GIVEN
        InMemoryClosableChannel channel = new InMemoryClosableChannel();
        when(logFile.getTransactionLogWriter())
                .thenReturn(new TransactionLogWriter(
                        channel, LATEST_KERNEL_VERSION_PROVIDER, BINARY_VERSIONS, LogRotation.NO_ROTATION));

        TransactionAppender appender = life.add(createTransactionAppender());

        // WHEN
        final byte[] additionalHeader = encodeLogIndex(5);
        final long timeStarted = 12345;
        long latestCommittedTxWhenStarted = 4545;
        long timeCommitted = timeStarted + 10;
        when(transactionIdStore.getLastCommittedTransactionId()).thenReturn(latestCommittedTxWhenStarted);

        LogEntryStart start = newStartEntry(
                LATEST_KERNEL_VERSION,
                0L,
                latestCommittedTxWhenStarted,
                latestCommittedTxWhenStarted + 8,
                0,
                additionalHeader,
                LogPosition.UNSPECIFIED);
        LogEntryCommit commit = newCommitEntry(
                LATEST_KERNEL_VERSION, latestCommittedTxWhenStarted + 2, timeCommitted, BASE_TX_CHECKSUM);
        CompleteBatchRepresentation transaction = new CompleteBatchRepresentation(start, singleTestCommand(), commit);

        var e = assertThrows(
                Exception.class,
                () -> appender.append(
                        new CompleteTransaction(
                                transaction,
                                NULL_CONTEXT,
                                StoreCursors.NULL,
                                new TransactionCommitment(transactionIdStore),
                                new IdStoreTransactionIdGenerator(transactionIdStore)),
                        LogAppendEvent.NULL));
        assertThat(e.getMessage()).contains("to be applied, but appending it ended up generating an");
    }

    @Test
    void shouldNotCallTransactionClosedOnFailedAppendedTransaction() throws Exception {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        final var logHeader = LATEST_LOG_FORMAT.newHeader(
                0,
                BASE_APPEND_INDEX,
                LogHeader.UNKNOWN_TERM,
                StoreId.UNKNOWN,
                512,
                BASE_TX_CHECKSUM,
                LATEST_KERNEL_VERSION);
        PhysicalLogVersionedStoreChannel logChannel = mock(PhysicalLogVersionedStoreChannel.class);
        when(logChannel.getLogFormatVersion()).thenReturn(LATEST_LOG_FORMAT);
        FlushableLogPositionAwareChannel channel =
                spy(new PhysicalFlushableLogPositionAwareChannel(logChannel, logHeader, INSTANCE));
        IOException failure = new IOException(failureMessage);
        doThrow(failure).when(channel).putVersion(anyByte());
        when(logFile.getTransactionLogWriter())
                .thenReturn(new TransactionLogWriter(
                        channel, LATEST_KERNEL_VERSION_PROVIDER, BINARY_VERSIONS, LogRotation.NO_ROTATION));

        when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);
        when(transactionIdStore.getLastCommittedTransaction())
                .thenReturn(new TransactionId(
                        txId,
                        txId + 2,
                        DEFAULT_BOOTSTRAP_VERSION,
                        BASE_TX_CHECKSUM,
                        BASE_TX_COMMIT_TIMESTAMP,
                        UNKNOWN_CONSENSUS_INDEX));
        Mockito.reset(databasePanic);
        TransactionAppender appender = life.add(createTransactionAppender());

        // WHEN
        CommandBatch transaction = mock(CommandBatch.class);
        when(transaction.consensusIndex()).thenReturn(0L);
        when(transaction.isFirst()).thenReturn(true);
        when(transaction.isLast()).thenReturn(true);
        when(transaction.kernelVersion()).thenReturn(LATEST_KERNEL_VERSION);

        var e = assertThrows(
                IOException.class,
                () -> appender.append(
                        new CompleteTransaction(
                                transaction,
                                NULL_CONTEXT,
                                StoreCursors.NULL,
                                new TransactionCommitment(transactionIdStore),
                                transactionIdGenerator),
                        LogAppendEvent.NULL));
        assertSame(failure, e);
        verify(transactionIdStore).nextCommittingTransactionId();
        verify(transactionIdStore, never())
                .transactionClosed(
                        eq(txId),
                        anyLong(),
                        eq(DEFAULT_BOOTSTRAP_VERSION),
                        anyLong(),
                        anyLong(),
                        anyInt(),
                        anyLong(),
                        anyLong());
        verify(databasePanic).panic(failure);
    }

    @Test
    void shouldNotCallTransactionClosedOnFailedForceLogToDisk() throws Exception {
        // GIVEN
        long txId = 3;
        String failureMessage = "Forces a failure";
        FlushableLogPositionAwareChannel channel = spy(new InMemoryClosableChannel());
        IOException failure = new IOException(failureMessage);
        final Flushable flushable = mock(Flushable.class);
        doAnswer(invocation -> {
                    invocation.callRealMethod();
                    return flushable;
                })
                .when(channel)
                .prepareForFlush();
        when(logFile.forceAfterAppend(any())).thenThrow(failure);
        when(logFile.getTransactionLogWriter())
                .thenReturn(new TransactionLogWriter(
                        channel, LATEST_KERNEL_VERSION_PROVIDER, BINARY_VERSIONS, LogRotation.NO_ROTATION));

        TransactionMetadataCache metadataCache = new TransactionMetadataCache();
        TransactionIdStore transactionIdStore = mock(TransactionIdStore.class);
        when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);
        when(transactionIdStore.getLastCommittedTransaction())
                .thenReturn(new TransactionId(
                        txId,
                        txId + 2,
                        DEFAULT_BOOTSTRAP_VERSION,
                        BASE_TX_CHECKSUM,
                        BASE_TX_COMMIT_TIMESTAMP,
                        UNKNOWN_CONSENSUS_INDEX));
        TransactionAppender appender = life.add(new BatchingTransactionAppender(
                logFiles, transactionIdStore, databasePanic, new SimpleAppendIndexProvider(), metadataCache));

        // WHEN
        CommandBatch commandBatch = mock(CommandBatch.class);
        when(commandBatch.consensusIndex()).thenReturn(0L);
        when(commandBatch.kernelVersion()).thenReturn(LATEST_KERNEL_VERSION);
        when(commandBatch.iterator()).thenReturn(emptyIterator());
        when(commandBatch.isFirst()).thenReturn(true);
        when(commandBatch.isLast()).thenReturn(true);

        var e = assertThrows(
                IOException.class,
                () -> appender.append(
                        new CompleteTransaction(
                                commandBatch,
                                NULL_CONTEXT,
                                StoreCursors.NULL,
                                new TransactionCommitment(transactionIdStore),
                                new IdStoreTransactionIdGenerator(transactionIdStore)),
                        LogAppendEvent.NULL));
        assertSame(failure, e);
        verify(transactionIdStore).nextCommittingTransactionId();
        verify(transactionIdStore, never())
                .transactionClosed(
                        eq(txId),
                        anyLong(),
                        eq(DEFAULT_BOOTSTRAP_VERSION),
                        anyLong(),
                        anyLong(),
                        anyInt(),
                        anyLong(),
                        anyLong());
    }

    @Test
    void shouldFailIfTransactionIdsMismatch() {
        // Given
        BatchingTransactionAppender appender = life.add(createTransactionAppender());
        var commitProcess = new InternalTransactionCommitProcess(
                appender,
                mock(StorageEngine.class, RETURNS_MOCKS),
                false,
                CommandCommitListeners.NO_LISTENERS,
                () -> true);
        when(transactionIdStore.nextCommittingTransactionId()).thenReturn(42L);
        var transactionCommitment = new TransactionCommitment(transactionIdStore);
        var transactionIdGenerator = new IdStoreTransactionIdGenerator(transactionIdStore);
        var transaction = new CompleteBatchRepresentation(
                newStartEntry(LATEST_KERNEL_VERSION, 1, 2, 3, 4, EMPTY_BYTE_ARRAY, LogPosition.UNSPECIFIED),
                singleTestCommand(),
                newCommitEntry(LATEST_KERNEL_VERSION, 11, 1L, BASE_TX_CHECKSUM));
        CompleteTransaction batch = new CompleteTransaction(
                transaction, NULL_CONTEXT, StoreCursors.NULL, transactionCommitment, transactionIdGenerator);
        assertThrows(
                TransactionFailureException.class,
                () -> commitProcess.commit(batch, TransactionWriteEvent.NULL, TransactionApplicationMode.EXTERNAL));
    }

    private void assertShouldAppendSingleTransaction(
            KernelVersion kernelVersion, BinarySupportedKernelVersions supportedKernelVersions) throws Exception {
        final var logAppendEvent = mock(LogAppendEvent.class);
        when(logAppendEvent.beginAppendTransaction(anyInt())).thenReturn(AppendTransactionEvent.NULL);

        when(logFile.forceAfterAppend(eq(logAppendEvent))).thenReturn(true);
        when(logFile.getLogRotation()).thenReturn(LogRotation.NO_ROTATION);

        CommandBatch transaction =
                transaction(singleTestCommand(kernelVersion), 5, 12345, 4545, 12345 + 10, kernelVersion);

        try (var writeChannel = getWriteChannel(fs, path, kernelVersion)) {
            when(logFile.getTransactionLogWriter())
                    .thenReturn(new TransactionLogWriter(
                            writeChannel, () -> kernelVersion, supportedKernelVersions, LogRotation.NO_ROTATION));
            long txId = 15;
            when(transactionIdStore.nextCommittingTransactionId()).thenReturn(txId);
            when(transactionIdStore.getLastCommittedTransaction())
                    .thenReturn(new TransactionId(
                            txId,
                            txId + 2,
                            DEFAULT_BOOTSTRAP_VERSION,
                            BASE_TX_CHECKSUM,
                            BASE_TX_COMMIT_TIMESTAMP,
                            UNKNOWN_CONSENSUS_INDEX));
            TransactionAppender appender = life.add(createTransactionAppender());

            appender.append(
                    new CompleteTransaction(
                            transaction, NULL_CONTEXT, StoreCursors.NULL, NO_COMMITMENT, TransactionIdGenerator.EMPTY),
                    logAppendEvent);
        }

        final LogEntryReader logEntryReader = logEntryReader(supportedKernelVersions);
        try (var readChannel = getReadChannel(fs, path, kernelVersion);
                CommittedCommandBatchCursor reader = new CommittedCommandBatchCursor(readChannel, logEntryReader)) {
            reader.next();
            CommittedCommandBatchRepresentation commandBatch = reader.get();
            CommandBatch tx = commandBatch.commandBatch();
            assertEquals(transaction.consensusIndex(), tx.consensusIndex());
            assertEquals(transaction.getTimeStarted(), tx.getTimeStarted());
            assertEquals(transaction.getTimeCommitted(), commandBatch.timeWritten());
            assertEquals(transaction.getLatestCommittedTxWhenStarted(), tx.getLatestCommittedTxWhenStarted());
        }

        if (kernelVersion.isLessThan(VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED)) {
            verify(logAppendEvent).setLogRotated(eq(false));
        } else {
            verify(logAppendEvent, never()).setLogRotated(anyBoolean());
        }
    }

    private BatchingTransactionAppender createTransactionAppender() {
        return new BatchingTransactionAppender(
                logFiles,
                transactionIdStore,
                databasePanic,
                new SimpleAppendIndexProvider(),
                new TransactionMetadataCache());
    }

    private static CommandBatch transaction(
            List<StorageCommand> commands,
            long consensusIndex,
            long timeStarted,
            long latestCommittedTxWhenStarted,
            long timeCommitted,
            KernelVersion kernelVersion) {
        return new CompleteCommandBatch(
                commands,
                consensusIndex,
                timeStarted,
                latestCommittedTxWhenStarted,
                timeCommitted,
                -1,
                kernelVersion,
                ANONYMOUS);
    }

    private static List<StorageCommand> singleTestCommand() {
        return Collections.singletonList(new TestCommand());
    }

    private static List<StorageCommand> singleTestCommand(KernelVersion kernelVersion) {
        return Collections.singletonList(new TestCommand(kernelVersion));
    }

    private CompleteTransaction batchOf(CommandBatch... transactions) {
        CompleteTransaction first = null;
        CompleteTransaction last = null;
        var transactionCommitment = new TransactionCommitment(transactionIdStore);
        for (CommandBatch transaction : transactions) {
            CompleteTransaction tx = new CompleteTransaction(
                    transaction, NULL_CONTEXT, StoreCursors.NULL, transactionCommitment, transactionIdGenerator);
            if (first == null) {
                first = last = tx;
            } else {
                last.next(tx);
                last = tx;
            }
        }
        return first;
    }

    private static class CountingVersionProvider implements KernelVersionProvider {
        private int versionLookedUp;

        @Override
        public KernelVersion kernelVersion() {
            versionLookedUp++;
            return LATEST_KERNEL_VERSION;
        }

        public int getVersionLookedUp() {
            return versionLookedUp;
        }
    }
}
