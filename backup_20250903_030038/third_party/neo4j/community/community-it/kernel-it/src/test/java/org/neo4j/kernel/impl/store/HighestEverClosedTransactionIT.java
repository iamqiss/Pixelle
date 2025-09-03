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
package org.neo4j.kernel.impl.store;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.collection.Dependencies.dependenciesOf;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.version.VersionStorageTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogFileCreateEvent;
import org.neo4j.kernel.impl.transaction.log.LogFileFlushEvent;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionRollbackEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionWriteEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.lock.LockTracer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
public class HighestEverClosedTransactionIT {
    @Inject
    private GraphDatabaseAPI db;

    @Inject
    private MetadataProvider metadataProvider;

    private PostCommitChecker postCommitCallback;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        postCommitCallback = new PostCommitChecker();
        var testTracers = new TransactionPostCommitTracers(postCommitCallback);
        builder.setExternalDependencies(dependenciesOf(testTracers));
    }

    @Test
    void closeTransactionBumpHighestEverClosedInfo() {
        long initialId = metadataProvider.getHighestEverClosedTransaction().id();
        int numberOfTransactions = 10;
        for (int i = 0; i < numberOfTransactions; i++) {
            try (Transaction transaction = db.beginTx()) {
                transaction.createNode();
                transaction.commit();
            }
        }
        assertEquals(
                initialId + numberOfTransactions,
                metadataProvider.getHighestEverClosedTransaction().id());
    }

    @Test
    void nonChunkedRolledBackTransactionDoesNotBumpHighestEverClosedInfo() {
        long initialId = metadataProvider.getHighestEverClosedTransaction().id();
        int numberOfTransactions = 10;
        for (int i = 0; i < numberOfTransactions; i++) {
            try (Transaction transaction = db.beginTx()) {
                transaction.createNode();
                transaction.rollback();
            }
        }
        assertEquals(
                initialId, metadataProvider.getHighestEverClosedTransaction().id());
    }

    @Test
    void initialClosedAndHighestClosedTransactionAreAligned() {
        var lastClosedTransaction = metadataProvider.getLastClosedTransaction();
        var highestClosedTransaction = metadataProvider.getHighestEverClosedTransaction();

        assertEquals(lastClosedTransaction.transactionId(), highestClosedTransaction);
    }

    @Test
    void highestEverClosedBatchIncrementedOnlyOnClose() {
        var initialHighestEver = metadataProvider.getHighestEverClosedTransaction();

        int numberOfTransactions = 10;
        for (int i = 0; i < numberOfTransactions; i++) {
            try (Transaction transaction = db.beginTx()) {
                transaction.createNode();
                TransactionId highestEverClosedTransaction = metadataProvider.getHighestEverClosedTransaction();
                postCommitCallback.setCheck(() ->
                        assertEquals(highestEverClosedTransaction, metadataProvider.getHighestEverClosedTransaction()));
                transaction.commit();
            }
        }

        var postTransactionHighestSeen = metadataProvider.getHighestEverClosedTransaction();
        assertEquals(initialHighestEver.id() + numberOfTransactions, postTransactionHighestSeen.id());
    }

    private static class TransactionPostCommitTracers implements Tracers {
        private final Runnable postCommitCallback;

        public TransactionPostCommitTracers(Runnable postCommitCallback) {
            this.postCommitCallback = postCommitCallback;
        }

        @Override
        public PageCacheTracer getPageCacheTracer() {
            return PageCacheTracer.NULL;
        }

        @Override
        public LockTracer getLockTracer() {
            return LockTracer.NONE;
        }

        @Override
        public DatabaseTracer getDatabaseTracer(NamedDatabaseId namedDatabaseId) {
            return new DatabaseTransactionPostCommitTracer(postCommitCallback);
        }

        @Override
        public VersionStorageTracer getVersionStorageTracer(NamedDatabaseId namedDatabaseId) {
            return VersionStorageTracer.NULL;
        }

        private static class DatabaseTransactionPostCommitTracer implements DatabaseTracer {
            private final Runnable postCommitCallback;

            public DatabaseTransactionPostCommitTracer(Runnable postCommitCallback) {
                this.postCommitCallback = postCommitCallback;
            }

            @Override
            public LogFileCreateEvent createLogFile() {
                return LogFileCreateEvent.NULL;
            }

            @Override
            public void openLogFile(Path filePath) {}

            @Override
            public void closeLogFile(Path filePath) {}

            @Override
            public LogAppendEvent logAppend() {
                return LogAppendEvent.NULL;
            }

            @Override
            public LogFileFlushEvent flushFile() {
                return LogFileFlushEvent.NULL;
            }

            @Override
            public long numberOfCheckPoints() {
                return 0;
            }

            @Override
            public long checkPointAccumulatedTotalTimeMillis() {
                return 0;
            }

            @Override
            public long lastCheckpointTimeMillis() {
                return 0;
            }

            @Override
            public long lastCheckpointPagesFlushed() {
                return 0;
            }

            @Override
            public long lastCheckpointIOs() {
                return 0;
            }

            @Override
            public long lastCheckpointIOLimit() {
                return 0;
            }

            @Override
            public long lastCheckpointIOLimitedTimes() {
                return 0;
            }

            @Override
            public long lastCheckpointIOLimitedMillis() {
                return 0;
            }

            @Override
            public long flushedBytes() {
                return 0;
            }

            @Override
            public TransactionEvent beginTransaction(CursorContext cursorContext, long transactionSequenceNumber) {
                return new TransactionEvent() {
                    @Override
                    public void setCommit(boolean commit) {}

                    @Override
                    public void setRollback(boolean rollback) {}

                    @Override
                    public TransactionWriteEvent beginCommitEvent() {
                        return new TransactionWriteEvent() {
                            @Override
                            public void close() {}

                            @Override
                            public LogAppendEvent beginLogAppend() {
                                return LogAppendEvent.NULL;
                            }

                            @Override
                            public StoreApplyEvent beginStoreApply() {
                                postCommitCallback.run();
                                return StoreApplyEvent.NULL;
                            }

                            @Override
                            public void chunkAppended(
                                    int chunkNumber, long transactionSequenceNumber, long transactionId) {}
                        };
                    }

                    @Override
                    public TransactionWriteEvent beginChunkWriteEvent() {
                        return TransactionWriteEvent.NULL;
                    }

                    @Override
                    public TransactionRollbackEvent beginRollback() {
                        return TransactionRollbackEvent.NULL;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public void setTransactionWriteState(String transactionWriteState) {}

                    @Override
                    public void setReadOnly(boolean wasReadOnly) {}

                    @Override
                    public void refreshVisibilityBoundary() {}
                };
            }

            @Override
            public TransactionWriteEvent beginAsyncCommit() {
                return TransactionWriteEvent.NULL;
            }

            @Override
            public TransactionRollbackEvent beginAsyncRollback() {
                return TransactionRollbackEvent.NULL;
            }

            @Override
            public long appendedBytes() {
                return 0;
            }

            @Override
            public long numberOfLogRotations() {
                return 0;
            }

            @Override
            public long logRotationAccumulatedTotalTimeMillis() {
                return 0;
            }

            @Override
            public long lastLogRotationTimeMillis() {
                return 0;
            }

            @Override
            public long numberOfFlushes() {
                return 0;
            }

            @Override
            public long lastTransactionLogAppendBatch() {
                return 0;
            }

            @Override
            public long batchesAppended() {
                return 0;
            }

            @Override
            public long rolledbackBatches() {
                return 0;
            }

            @Override
            public long rolledbackBatchedTransactions() {
                return 0;
            }

            @Override
            public LogCheckPointEvent beginCheckPoint() {
                return LogCheckPointEvent.NULL;
            }
        }
    }

    private static class PostCommitChecker implements Runnable {
        private volatile Runnable check;

        @Override
        public void run() {
            var localCheck = check;
            if (localCheck != null) {
                localCheck.run();
            }
        }

        public void setCheck(Runnable check) {
            this.check = check;
        }
    }
}
