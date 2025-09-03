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
package org.neo4j.router.impl.transaction.database;

import java.util.HashSet;
import java.util.Set;
import org.neo4j.cypher.internal.util.InternalNotification;
import org.neo4j.exceptions.KernelException;
import org.neo4j.fabric.bookmark.LocalBookmark;
import org.neo4j.fabric.bookmark.LocalGraphTransactionIdTracker;
import org.neo4j.fabric.bookmark.TransactionBookmarkManager;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.fabric.executor.TaggingPlanDescriptionWrapper;
import org.neo4j.function.ThrowingAction;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.GqlStatusObject;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionMonitor;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.notifications.NotificationWrapping;
import org.neo4j.router.impl.subscriber.DelegatingQueryExecution;
import org.neo4j.router.impl.subscriber.StatementLifecycleQuerySubscriber;
import org.neo4j.router.query.Query;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.TransactionInfo;
import scala.Option;

public class LocalDatabaseTransaction implements DatabaseTransaction {
    private final Location.Local location;
    private final TransactionInfo transactionInfo;
    private final InternalTransaction internalTransaction;
    private final TransactionalContextFactory transactionalContextFactory;
    private final QueryExecutionEngine queryExecutionEngine;
    private final TransactionBookmarkManager bookmarkManager;
    private final LocalGraphTransactionIdTracker transactionIdTracker;
    private ConstituentTransactionFactory constituentTransactionFactory;
    private final Set<TransactionalContext> openExecutionContexts = new HashSet<>();

    public LocalDatabaseTransaction(
            Location.Local location,
            TransactionInfo transactionInfo,
            InternalTransaction internalTransaction,
            TransactionalContextFactory transactionalContextFactory,
            QueryExecutionEngine queryExecutionEngine,
            TransactionBookmarkManager bookmarkManager,
            LocalGraphTransactionIdTracker transactionIdTracker,
            ConstituentTransactionFactory constituentTransactionFactory) {
        this.location = location;
        this.transactionInfo = transactionInfo;
        this.internalTransaction = internalTransaction;
        this.transactionalContextFactory = transactionalContextFactory;
        this.queryExecutionEngine = queryExecutionEngine;
        this.bookmarkManager = bookmarkManager;
        this.transactionIdTracker = transactionIdTracker;
        this.constituentTransactionFactory = constituentTransactionFactory;
    }

    public void setConstituentTransactionFactory(ConstituentTransactionFactory constituentTransactionFactory) {
        this.constituentTransactionFactory = constituentTransactionFactory;
    }

    @Override
    public Location location() {
        return location;
    }

    @Override
    public void commit() {
        closeContexts();
        if (internalTransaction.isOpen()) {
            translateLocalError(() -> internalTransaction.commit());
        }

        long transactionId = transactionIdTracker.getTransactionId(location);
        bookmarkManager.localTransactionCommitted(location, new LocalBookmark(transactionId));
    }

    @Override
    public void rollback() {
        closeContexts();
        if (internalTransaction.isOpen()) {
            translateLocalError(internalTransaction::rollback);
        }
    }

    @Override
    public void close() {
        closeContexts();
        if (internalTransaction.isOpen()) {
            translateLocalError(internalTransaction::close);
        }
    }

    @Override
    public void terminate(Status reason) {
        terminateIfPossible(reason);
    }

    public void terminateIfPossible(Status reason) {
        if (internalTransaction.isOpen()
                && internalTransaction.terminationReason().isEmpty()) {
            internalTransaction.terminate(reason);
        }
    }

    @Override
    public QueryExecution executeQuery(
            Query query,
            QuerySubscriber querySubscriber,
            QueryStatementLifecycles.StatementLifecycle statementLifecycle,
            Set<InternalNotification> routerNotifications) {
        return translateLocalError(() -> {
            var transactionalContext = transactionalContextFactory.newContextForQuery(
                    internalTransaction,
                    statementLifecycle.getMonitoredQuery(),
                    transactionInfo.queryExecutionConfiguration(),
                    constituentTransactionFactory);
            statementLifecycle.startExecution(true);
            openExecutionContexts.add(transactionalContext);
            var execution = queryExecutionEngine.executeQuery(
                    query.text(),
                    query.parameters(),
                    transactionalContext,
                    true,
                    new QuerySubscriberImpl(transactionalContext, querySubscriber, statementLifecycle),
                    QueryExecutionMonitor.NO_OP);
            return new TransactionalContextQueryExecution(execution, transactionalContext, routerNotifications);
        });
    }

    private void translateLocalError(ThrowingAction<KernelException> throwingAction) {
        try {
            throwingAction.apply();
        } catch (KernelException kernelException) {
            throw FabricException.translateLocalError(kernelException);
        }
    }

    private <T> T translateLocalError(ThrowingSupplier<T, KernelException> throwingSupplier) {
        try {
            return throwingSupplier.get();
        } catch (KernelException kernelException) {
            throw FabricException.translateLocalError(kernelException);
        }
    }

    private void closeContexts() {
        openExecutionContexts.forEach(TransactionalContext::close);
    }

    public InternalTransaction internalTransaction() {
        return internalTransaction;
    }

    private class TransactionalContextQueryExecution extends DelegatingQueryExecution {

        private final TransactionalContext transactionalContext;
        private final Set<InternalNotification> notifications;

        TransactionalContextQueryExecution(
                QueryExecution queryExecution,
                TransactionalContext transactionalContext,
                Set<InternalNotification> notifications) {
            super(queryExecution);
            this.transactionalContext = transactionalContext;
            this.notifications = notifications;
        }

        @Override
        public void cancel() {
            super.cancel();
            openExecutionContexts.remove(transactionalContext);
            transactionalContext.close();
        }

        @Override
        public ExecutionPlanDescription executionPlanDescription() {
            return new TaggingPlanDescriptionWrapper(super.executionPlanDescription(), location.getDatabaseName());
        }

        @Override
        public Iterable<Notification> getNotifications() {
            var additionalNotifications = notifications.stream()
                    .map(n -> NotificationWrapping.asKernelNotificationJava(Option.empty(), n))
                    .toList();
            return Iterables.concat(super.getNotifications(), additionalNotifications);
        }

        @Override
        public Iterable<GqlStatusObject> getGqlStatusObjects() {
            var additionalNotifications = notifications.stream()
                    .map(n -> NotificationWrapping.asKernelNotificationJava(Option.empty(), n))
                    .toList();
            return Iterables.concat(super.getGqlStatusObjects(), additionalNotifications);
        }
    }

    @Override
    public boolean isOpen() {
        return internalTransaction.isOpen();
    }

    private class QuerySubscriberImpl extends StatementLifecycleQuerySubscriber {

        private final TransactionalContext transactionalContext;

        public QuerySubscriberImpl(
                TransactionalContext transactionalContext,
                QuerySubscriber querySubscriber,
                QueryStatementLifecycles.StatementLifecycle statementLifecycle) {
            super(querySubscriber, statementLifecycle);
            this.transactionalContext = transactionalContext;
        }

        @Override
        public void onResultCompleted(QueryStatistics statistics) {
            super.onResultCompleted(statistics);
            openExecutionContexts.remove(transactionalContext);
            transactionalContext.close();
        }

        @Override
        public void onError(Throwable throwable) throws Exception {
            super.onError(throwable);
            openExecutionContexts.remove(transactionalContext);
            transactionalContext.close();
        }
    }
}
