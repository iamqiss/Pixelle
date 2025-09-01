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
import static org.neo4j.configuration.GraphDatabaseInternalSettings.shutdown_terminated_transaction_wait_timeout;

import java.time.Duration;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

@TestDirectoryExtension
class DatabaseTransactionShutdownIT {

    @Inject
    TestDirectory directory;

    FakeClock clock = Clocks.fakeClock();
    DatabaseManagementService dbms;
    GraphDatabaseAPI db;

    @BeforeEach
    void setUp() {
        dbms = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .setConfig(GraphDatabaseSettings.shutdown_transaction_end_timeout, Duration.ofMillis(0))
                .setClock(clock)
                .build();
        db = (GraphDatabaseAPI) dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
    }

    @AfterEach
    void tearDown() {
        shutdownDbms();
    }

    @Test
    void shouldWaitForTransactionToDetectTerminationOnShutdown() throws Exception {
        // Given
        Duration waitTime = db.getDependencyResolver()
                .resolveDependency(Config.class)
                .get(shutdown_terminated_transaction_wait_timeout);

        try (OtherThreadExecutor executor = new OtherThreadExecutor("test")) {
            Transaction tx = db.beginTx();
            KernelTransaction ktx = ((TransactionImpl) tx).kernelTransaction();
            // When
            tx.createNode();
            Future<Object> shutdownFuture = executor.executeDontWait(this::shutdownDbms);
            executor.waitUntilWaiting(details -> details.isAt(AbstractDatabase.class, "awaitAllClosingTransactions"));

            // Then
            assertThat(ktx.getTerminationMark()).isNotEmpty();
            assertThatThrownBy(tx::createNode).isInstanceOf(TransactionTerminatedException.class);

            // Forward the clock
            clock.forward(waitTime.plusMillis(1));

            // Shutdown will continue, ignoring the non-closed transaction
            shutdownFuture.get();

            // Close throws as dbms is shut down
            assertThatThrownBy(tx::close).isInstanceOf(TransactionFailureException.class);
        }
    }

    @Test
    void shouldWaitForTransactionToDetectTerminationAndCloseOnShutdown() throws Exception {
        // Given
        try (OtherThreadExecutor executor = new OtherThreadExecutor("test")) {
            Future<Object> shutdownFuture;
            Transaction tx = db.beginTx();
            KernelTransaction ktx = ((TransactionImpl) tx).kernelTransaction();
            // When
            tx.createNode();
            shutdownFuture = executor.executeDontWait(this::shutdownDbms);
            executor.waitUntilWaiting(details -> details.isAt(AbstractDatabase.class, "awaitAllClosingTransactions"));
            assertThat(ktx.getTerminationMark()).isNotEmpty();

            // Transaction is terminated. Let's close it.
            tx.close();
            // Shutdown then continues. Note: we don't forward the clock
            shutdownFuture.get();
        }
    }

    private Void shutdownDbms() {
        if (dbms != null) {
            dbms.shutdown();
            dbms = null;
        }
        return null;
    }
}
