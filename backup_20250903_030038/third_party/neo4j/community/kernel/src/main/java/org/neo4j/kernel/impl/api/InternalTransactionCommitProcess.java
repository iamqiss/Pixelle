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
package org.neo4j.kernel.impl.api;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionCommitFailed;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionLogError;

import java.util.function.BooleanSupplier;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.OutOfDiskSpaceException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineTransaction;
import org.neo4j.storageengine.api.TransactionApplicationMode;

public class InternalTransactionCommitProcess implements TransactionCommitProcess {
    private final TransactionAppender appender;
    private final StorageEngine storageEngine;
    private final boolean preAllocateSpaceInStores;
    private final CommandCommitListeners commandCommitListeners;
    private final BooleanSupplier prefetchCommands;

    public InternalTransactionCommitProcess(
            TransactionAppender appender,
            StorageEngine storageEngine,
            boolean preAllocateSpaceInStores,
            CommandCommitListeners commandCommitListeners,
            BooleanSupplier prefetchCommands) {
        this.appender = appender;
        this.storageEngine = storageEngine;
        this.preAllocateSpaceInStores = preAllocateSpaceInStores;
        this.commandCommitListeners = commandCommitListeners;
        this.prefetchCommands = prefetchCommands;
    }

    @Override
    public long commit(
            StorageEngineTransaction batch,
            TransactionWriteEvent transactionWriteEvent,
            TransactionApplicationMode mode)
            throws TransactionFailureException {
        try {
            if (preAllocateSpaceInStores) {
                preAllocateSpaceInStores(batch, transactionWriteEvent, mode);
            }
            if (prefetchCommands.getAsBoolean()) {
                storageEngine.prefetchPagesForCommands(batch, mode);
            }

            long lastAppendIndex = appendToLog(batch, transactionWriteEvent);
            try {
                applyToStore(batch, transactionWriteEvent, mode);
            } finally {
                close(batch);
            }

            commandCommitListeners.registerSuccess(batch, lastAppendIndex);
            return lastAppendIndex;
        } catch (Exception e) {
            commandCommitListeners.registerFailure(batch, e);
            throw e;
        }
    }

    private long appendToLog(StorageEngineTransaction batch, TransactionWriteEvent transactionWriteEvent)
            throws TransactionFailureException {
        try (LogAppendEvent logAppendEvent = transactionWriteEvent.beginLogAppend()) {
            return appender.append(batch, logAppendEvent);
        } catch (Throwable cause) {
            throw new TransactionFailureException(
                    TransactionLogError, cause, "Could not append transaction: " + batch + " to log.");
        }
    }

    protected void applyToStore(
            StorageEngineTransaction batch,
            TransactionWriteEvent transactionWriteEvent,
            TransactionApplicationMode mode)
            throws TransactionFailureException {
        try (StoreApplyEvent storeApplyEvent = transactionWriteEvent.beginStoreApply()) {
            storageEngine.apply(batch, mode);
        } catch (Throwable cause) {
            throw new TransactionFailureException(
                    TransactionCommitFailed,
                    cause,
                    "Could not apply the transaction: " + batch + " to the store after written to log.");
        }
    }

    private void preAllocateSpaceInStores(
            StorageEngineTransaction batch,
            TransactionWriteEvent transactionWriteEvent,
            TransactionApplicationMode mode)
            throws TransactionFailureException {
        // FIXME ODP - add function to commitEvent to be able to trace?
        try {
            storageEngine.preAllocateStoreFilesForCommands(batch, mode);
        } catch (OutOfDiskSpaceException oods) {
            throw new TransactionFailureException(
                    // FIXME ODP - add an out of disk space status when we are ready to expose this functionality
                    Status.General.UnknownError,
                    oods,
                    "Could not preallocate disk space for the transaction: " + batch);
        } catch (Throwable cause) {
            throw new TransactionFailureException(
                    TransactionCommitFailed, cause, "Could not preallocate disk space for the transaction: " + batch);
        }
    }

    private static void close(StorageEngineTransaction batch) {
        while (batch != null) {
            batch.close();
            batch = batch.next();
        }
    }
}
