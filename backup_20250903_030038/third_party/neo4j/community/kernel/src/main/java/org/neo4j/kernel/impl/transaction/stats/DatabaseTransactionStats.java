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
package org.neo4j.kernel.impl.transaction.stats;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.kernel.impl.monitoring.TransactionMonitor;
import org.neo4j.kernel.impl.monitoring.TransactionSizeMonitor;

public class DatabaseTransactionStats implements TransactionMonitor, TransactionCounters {
    @FunctionalInterface
    public interface Factory {
        DatabaseTransactionStats create();
    }

    private final AtomicLong activeReadTransactionCount = new AtomicLong();
    private final LongAdder startedTransactionCount = new LongAdder();
    private final LongAdder activeWriteTransactionCount = new LongAdder();
    private final LongAdder committedReadTransactionCount = new LongAdder();
    private final LongAdder committedWriteTransactionCount = new LongAdder();
    private final LongAdder rolledBackReadTransactionCount = new LongAdder();
    private final LongAdder rolledBackWriteTransactionCount = new LongAdder();
    private final LongAdder terminatedReadTransactionCount = new LongAdder();
    private final LongAdder terminatedWriteTransactionCount = new LongAdder();
    private final LongAdder totalTransactionsValidationFailures = new LongAdder();
    private final LongAdder totalTransactionsRetries = new LongAdder();
    private final AtomicLong peakTransactionCount = new AtomicLong();
    private volatile TransactionSizeMonitor transactionSizeCallback = NullTransactionSizeCallback.INSTANCE;

    @Override
    public void transactionStarted() {
        startedTransactionCount.increment();
        long active = activeReadTransactionCount.incrementAndGet();
        peakTransactionCount.updateAndGet(peak -> Math.max(peak, active));
    }

    @Override
    public void transactionFinished(boolean committed, boolean write) {
        if (write) {
            activeWriteTransactionCount.decrement();
        } else {
            activeReadTransactionCount.decrementAndGet();
        }
        if (committed) {
            incrementCounter(committedReadTransactionCount, committedWriteTransactionCount, write);
        } else {
            incrementCounter(rolledBackReadTransactionCount, rolledBackWriteTransactionCount, write);
        }
    }

    @Override
    public void transactionTerminated(boolean write) {
        incrementCounter(terminatedReadTransactionCount, terminatedWriteTransactionCount, write);
    }

    @Override
    public void upgradeToWriteTransaction() {
        activeReadTransactionCount.decrementAndGet();
        activeWriteTransactionCount.increment();
    }

    @Override
    public void transactionValidationFailure(DatabaseFile databaseFile) {
        totalTransactionsValidationFailures.increment();
    }

    @Override
    public void transactionRetry() {
        totalTransactionsRetries.increment();
    }

    @Override
    public long getPeakConcurrentNumberOfTransactions() {
        return peakTransactionCount.longValue();
    }

    @Override
    public long getNumberOfStartedTransactions() {
        return startedTransactionCount.longValue();
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return getNumberOfCommittedReadTransactions() + getNumberOfCommittedWriteTransactions();
    }

    @Override
    public long getNumberOfCommittedReadTransactions() {
        return committedReadTransactionCount.longValue();
    }

    @Override
    public long getNumberOfCommittedWriteTransactions() {
        return committedWriteTransactionCount.longValue();
    }

    @Override
    public long getNumberOfActiveTransactions() {
        return getNumberOfActiveReadTransactions() + getNumberOfActiveWriteTransactions();
    }

    @Override
    public long getNumberOfActiveReadTransactions() {
        return activeReadTransactionCount.longValue();
    }

    @Override
    public long getNumberOfActiveWriteTransactions() {
        return activeWriteTransactionCount.longValue();
    }

    @Override
    public long getNumberOfTerminatedTransactions() {
        return getNumberOfTerminatedReadTransactions() + getNumberOfTerminatedWriteTransactions();
    }

    @Override
    public long getNumberOfTerminatedReadTransactions() {
        return terminatedReadTransactionCount.longValue();
    }

    @Override
    public long getNumberOfTerminatedWriteTransactions() {
        return terminatedWriteTransactionCount.longValue();
    }

    @Override
    public long getNumberOfRolledBackTransactions() {
        return getNumberOfRolledBackReadTransactions() + getNumberOfRolledBackWriteTransactions();
    }

    @Override
    public long getNumberOfRolledBackReadTransactions() {
        return rolledBackReadTransactionCount.longValue();
    }

    @Override
    public long getNumberOfRolledBackWriteTransactions() {
        return rolledBackWriteTransactionCount.longValue();
    }

    @Override
    public long totalTransactionsValidationFailures() {
        return totalTransactionsValidationFailures.longValue();
    }

    @Override
    public long totalTransactionsRetries() {
        return totalTransactionsRetries.longValue();
    }

    @Override
    public void setTransactionSizeCallback(TransactionSizeMonitor transactionSizeMonitor) {
        this.transactionSizeCallback =
                transactionSizeMonitor != null ? transactionSizeMonitor : NullTransactionSizeCallback.INSTANCE;
    }

    @Override
    public void addHeapTransactionSize(long transactionSizeHeap) {
        transactionSizeCallback.addHeapTransactionSize(transactionSizeHeap);
    }

    @Override
    public void addNativeTransactionSize(long transactionSizeNative) {
        transactionSizeCallback.addNativeTransactionSize(transactionSizeNative);
    }

    private static void incrementCounter(LongAdder readCount, LongAdder writeCount, boolean write) {
        if (write) {
            writeCount.increment();
        } else {
            readCount.increment();
        }
    }

    private static class NullTransactionSizeCallback implements TransactionSizeMonitor {
        private static final TransactionSizeMonitor INSTANCE = new NullTransactionSizeCallback();

        private NullTransactionSizeCallback() {}

        @Override
        public void addHeapTransactionSize(long transactionSizeHeap) {}

        @Override
        public void addNativeTransactionSize(long transactionSizeNative) {}
    }
}
