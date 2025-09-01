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
package org.neo4j.bolt.protocol.common.fsm.transition.ready;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.transaction.AbstractTransactionInitiatingMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;
import org.neo4j.values.virtual.MapValue;

abstract class ResolveHomeDatabaseStateTransitionTest<M extends AbstractTransactionInitiatingMessage>
        extends AbstractStateTransitionTest<M, ResolveHomeDatabaseStateTransition<M>> {

    @Test
    void shouldCallResolveHomeDatabaseStateTransition() throws Exception {
        var message = createMessage(null, null);

        Assertions.assertNull(getTransition().process(this.context, message, this.responseHandler));

        Mockito.verify(this.context.connection(), Mockito.only()).resolveDefaultDatabase();
    }

    @TestFactory
    Stream<DynamicTest> testShouldNotResolveHomeDatabaseStateTransition() throws Exception {
        return Stream.of(createMessage("mydb", null), createMessage(null, "myimp"), createMessage("mydb", "myimp"))
                .map(message -> DynamicTest.dynamicTest(
                        String.format(
                                "database: %s, impersonatedUser: %s",
                                message.databaseName(), message.impersonatedUser()),
                        () -> {
                            Assertions.assertNull(getTransition().process(this.context, message, this.responseHandler));

                            Mockito.verify(this.context.connection(), Mockito.never())
                                    .resolveDefaultDatabase();
                        }));
    }

    @Nested
    public static class ResolveHomeDatabaseStateTransitionOnAutoCommitTest
            extends ResolveHomeDatabaseStateTransitionTest<RunMessage> {
        @Override
        protected ResolveHomeDatabaseStateTransition<RunMessage> getTransition() {
            return ResolveHomeDatabaseStateTransition.getInstanceForAutoCommit();
        }

        @Override
        protected RunMessage createMessage(String database, String impersonatedUser) {
            return new RunMessage(
                    "statement",
                    MapValue.EMPTY,
                    List.of(),
                    Duration.ZERO,
                    AccessMode.READ,
                    Map.of(),
                    database,
                    impersonatedUser,
                    null);
        }
    }

    @Nested
    public static class ResolveHomeDatabaseStateTransitionForBeginTest
            extends ResolveHomeDatabaseStateTransitionTest<BeginMessage> {
        @Override
        protected ResolveHomeDatabaseStateTransition<BeginMessage> getTransition() {
            return ResolveHomeDatabaseStateTransition.getInstanceForBegin();
        }

        @Override
        protected BeginMessage createMessage(String database, String impersonatedUser) {
            return new BeginMessage(List.of(), Duration.ZERO, AccessMode.READ, Map.of(), database, impersonatedUser);
        }
    }

    protected abstract M createMessage(String database, String impersonatedUser);
}
