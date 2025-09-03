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
package org.neo4j.kernel.impl.api.commit;

import static java.util.Collections.emptyList;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.util.List;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.TransactionRollbackException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.TransactionClockContext;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.chunk.ChunkMetadata;
import org.neo4j.kernel.impl.api.chunk.ChunkedCommandBatch;
import org.neo4j.kernel.impl.api.chunk.ChunkedTransaction;
import org.neo4j.kernel.impl.api.chunk.TransactionRollbackProcess;
import org.neo4j.kernel.impl.api.transaction.serial.SerialExecutionGuard;
import org.neo4j.kernel.impl.api.txid.TransactionIdGenerator;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.transaction.log.TransactionCommitmentFactory;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.lock.LockTracer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;
import org.neo4j.storageengine.api.txstate.validation.TransactionValidator;
import org.neo4j.storageengine.api.txstate.validation.ValidationLockDumper;

public final class ChunkCommitter implements TransactionCommitter {
    private final KernelTransactionImplementation ktx;
    private int chunkNumber = BASE_CHUNK_ID;
    private long previousBatchAppendIndex = UNKNOWN_APPEND_INDEX;
    private KernelVersion kernelVersion;
    private ChunkedTransaction transactionPayload;
    private final TransactionCommitmentFactory commitmentFactory;
    private final KernelVersionProvider kernelVersionProvider;
    private final StoreCursors transactionalCursors;
    private final TransactionIdGenerator transactionIdGenerator;
    private final TransactionCommitProcess commitProcess;
    private final DatabaseHealth databaseHealth;
    private final TransactionClockContext clocks;
    private final TransactionRollbackProcess chunkedRollbackProcess;
    private final TransactionValidator transactionValidator;
    private final ValidationLockDumper validationLockDumper;
    private final SerialExecutionGuard serialExecutionGuard;
    private final Log log;
    private final HostedOnMode mode;
    private long lastTransactionIdWhenStarted;
    private long startTimeMillis;
    private LeaseClient leaseClient;

    public ChunkCommitter(
            KernelTransactionImplementation ktx,
            TransactionCommitmentFactory commitmentFactory,
            KernelVersionProvider kernelVersionProvider,
            StoreCursors transactionalCursors,
            TransactionIdGenerator transactionIdGenerator,
            TransactionCommitProcess commitProcess,
            DatabaseHealth databaseHealth,
            TransactionClockContext clocks,
            TransactionRollbackProcess chunkedRollbackProcess,
            TransactionValidator transactionValidator,
            ValidationLockDumper validationLockDumper,
            SerialExecutionGuard serialExecutionGuard,
            LogProvider logProvider,
            HostedOnMode mode) {
        this.ktx = ktx;
        this.commitmentFactory = commitmentFactory;
        this.kernelVersionProvider = kernelVersionProvider;
        this.transactionalCursors = transactionalCursors;
        this.transactionIdGenerator = transactionIdGenerator;
        this.commitProcess = commitProcess;
        this.databaseHealth = databaseHealth;
        this.clocks = clocks;
        this.chunkedRollbackProcess = chunkedRollbackProcess;
        this.transactionValidator = transactionValidator;
        this.validationLockDumper = validationLockDumper;
        this.serialExecutionGuard = serialExecutionGuard;
        this.log = logProvider.getLog(ChunkCommitter.class);
        this.mode = mode;
    }

    @Override
    public long commit(
            TransactionWriteEvent transactionWriteEvent,
            LeaseClient leaseClient,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            KernelTransaction.KernelTransactionMonitor kernelTransactionMonitor,
            LockTracer lockTracer,
            long commitTime,
            long startTimeMillis,
            long lastTransactionIdWhenStarted,
            boolean commit,
            TransactionApplicationMode mode)
            throws KernelException {
        LockManager.Client lockClient = ktx.lockClient();
        try {
            List<StorageCommand> extractedCommands = ktx.extractCommands(memoryTracker);
            if (!extractedCommands.isEmpty() || (commit && transactionPayload != null)) {
                serialExecutionGuard.check();
                if (kernelVersion == null) {
                    this.kernelVersion = kernelVersionProvider.kernelVersion();
                    this.lastTransactionIdWhenStarted = lastTransactionIdWhenStarted;
                    this.startTimeMillis = lastTransactionIdWhenStarted;
                    this.leaseClient = leaseClient;
                }
                if (commit) {
                    validateCurrentKernelVersion();
                }
                try {
                    transactionValidator.validate(
                            extractedCommands, cursorContext, lockClient, lockTracer, validationLockDumper);
                    var chunkMetadata = new ChunkMetadata(
                            chunkNumber == BASE_CHUNK_ID,
                            commit,
                            false,
                            previousBatchAppendIndex,
                            chunkNumber,
                            new MutableLong(UNKNOWN_CONSENSUS_INDEX),
                            new MutableLong(UNKNOWN_APPEND_INDEX),
                            startTimeMillis,
                            lastTransactionIdWhenStarted,
                            commitTime,
                            leaseClient.leaseId(),
                            kernelVersion,
                            ktx.securityContext().subject().userSubject());
                    var transaction = getTransaction(cursorContext);

                    ChunkedCommandBatch chunk = new ChunkedCommandBatch(extractedCommands, chunkMetadata);
                    transaction.init(chunk);
                    commitProcess.commit(transaction, transactionWriteEvent, mode);

                    // transaction chunk commit completed
                    transactionPayload = transaction;

                    validationLockDumper.dumpLocks(
                            transactionValidator, lockClient, chunkNumber, transactionPayload.transactionId());
                    transactionWriteEvent.chunkAppended(
                            chunkNumber, ktx.getTransactionSequenceNumber(), transactionPayload.transactionId());
                } catch (TransactionConflictException tce) {
                    throw tce;
                } catch (Exception e) {
                    log.debug("Transaction chunk commit failure.", e);
                    throw e;
                }
                previousBatchAppendIndex = transactionPayload.lastBatchAppendIndex();
                chunkNumber++;
            }
            return transactionPayload != null ? transactionPayload.transactionId() : KernelTransaction.READ_ONLY_ID;
        } finally {
            lockClient.reset();
        }
    }

    @Override
    public void rollback(TransactionRollbackEvent rollbackEvent) {
        if (transactionPayload != null) {
            try {
                validateCurrentKernelVersion();
                prepareRollBackEntry();
                if (isSingleInstance()) {
                    chunkedRollbackProcess.rollbackChunks(transactionPayload, rollbackEvent);
                }
                writeRollbackEntry(rollbackEvent);
            } catch (Exception e) {
                if (isSingleInstance()) {
                    databaseHealth.panic(e);
                }
                Exceptions.throwIfInstanceOf(e, TransactionRollbackException.class);
                throw new TransactionRollbackException("Transaction rollback failed", e);
            }
        }
    }

    private boolean isSingleInstance() {
        return HostedOnMode.SINGLE == mode;
    }

    private ChunkedTransaction getTransaction(CursorContext cursorContext) {
        return transactionPayload != null
                ? transactionPayload
                : new ChunkedTransaction(
                        cursorContext,
                        ktx.getTransactionSequenceNumber(),
                        transactionalCursors,
                        commitmentFactory.newCommitment(),
                        transactionIdGenerator);
    }

    private void writeRollbackEntry(TransactionRollbackEvent transactionRollbackEvent)
            throws TransactionFailureException {
        try (var writeEvent = transactionRollbackEvent.beginRollbackWriteEvent()) {
            commitProcess.commit(transactionPayload, writeEvent, INTERNAL);
        }
    }

    private void prepareRollBackEntry() {
        var chunkMetadata = new ChunkMetadata(
                false,
                true,
                true,
                UNKNOWN_APPEND_INDEX,
                chunkNumber,
                new MutableLong(UNKNOWN_CONSENSUS_INDEX),
                new MutableLong(UNKNOWN_APPEND_INDEX),
                startTimeMillis,
                lastTransactionIdWhenStarted,
                clocks.systemClock().millis(),
                leaseClient.leaseId(),
                kernelVersion,
                ktx.securityContext().subject().userSubject());
        ChunkedCommandBatch chunk = new ChunkedCommandBatch(emptyList(), chunkMetadata);
        transactionPayload.init(chunk);
    }

    // kernel version can be updated by upgrade listener and for now we only fail to commit such
    // transactions.
    private void validateCurrentKernelVersion() {
        if (kernelVersion != kernelVersionProvider.kernelVersion()) {
            throw new UnsupportedOperationException("We do not support upgrade during chunked transaction.");
        }
    }

    @Override
    public void reset() {
        chunkNumber = BASE_CHUNK_ID;
        kernelVersion = null;
        transactionPayload = null;
        lastTransactionIdWhenStarted = 0;
        startTimeMillis = 0;
        leaseClient = null;
    }
}
