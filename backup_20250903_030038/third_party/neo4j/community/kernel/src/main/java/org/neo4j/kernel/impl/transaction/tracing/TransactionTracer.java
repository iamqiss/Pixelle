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
package org.neo4j.kernel.impl.transaction.tracing;

import org.neo4j.io.pagecache.context.CursorContext;

/**
 * The TransactionTracer is the root of the tracer hierarchy that gets notified about the life of transactions. The
 * events encapsulate the entire life of each transaction, but most of the events are concerned with what goes on
 * during commit. Implementers should take great care to make their implementations as fast as possible. Note that
 * tracers are not allowed to throw exceptions.
 */
public interface TransactionTracer {
    /**
     * A TransactionTracer implementation that does nothing, other than return the NULL variants of the companion
     * interfaces.
     */
    TransactionTracer NULL = new TransactionTracer() {

        @Override
        public TransactionEvent beginTransaction(CursorContext cursorContext, long transactionSequenceNumber) {
            return TransactionEvent.NULL;
        }

        @Override
        public TransactionWriteEvent beginAsyncCommit() {
            return TransactionWriteEvent.NULL;
        }

        @Override
        public TransactionRollbackEvent beginAsyncRollback() {
            return TransactionRollbackEvent.NULL;
        }
    };

    /**
     * A transaction starts.
     * @return An event that represents the transaction.
     * @param cursorContext page cursor context that used by transaction
     * @param transactionSequenceNumber transaction sequence number
     */
    TransactionEvent beginTransaction(CursorContext cursorContext, long transactionSequenceNumber);

    /**
     * A commit starts in a context where it is detached from the
     * execution of a transaction, for example when transactions
     * are replicated in a cluster setting.
     *
     * @return An event that represents the commit.
     */
    TransactionWriteEvent beginAsyncCommit();

    /**
     * Rollback event of transaction that is replicated in a cluster commit.
     * @return rollback event
     */
    TransactionRollbackEvent beginAsyncRollback();
}
