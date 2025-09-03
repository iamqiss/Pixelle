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

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.fsm.state.transition.AbstractStateTransition;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.transaction.AbstractTransactionInitiatingMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.BeginMessage;
import org.neo4j.bolt.protocol.common.message.request.transaction.RunMessage;

/**
 * Resolves the HomeDatabaseResolution case the message is not impersonating and doesn't have database set.
 * <p/>
 * Used in combination {@link CreateTransactionStateTransition} and {@link CreateAutocommitStatementStateTransition}
 * from Bolt Protocol 5.8. This transition is added before the main transition and it doesn't perform any change on the
 * current state.
 */
public class ResolveHomeDatabaseStateTransition<M extends AbstractTransactionInitiatingMessage>
        extends AbstractStateTransition<M> {
    private static final ResolveHomeDatabaseStateTransition<BeginMessage> BEGIN_INSTANCE =
            new ResolveHomeDatabaseStateTransition<>(BeginMessage.class);
    private static final ResolveHomeDatabaseStateTransition<RunMessage> RUN_INSTANCE =
            new ResolveHomeDatabaseStateTransition<>(RunMessage.class);

    private ResolveHomeDatabaseStateTransition(Class<M> requestType) {
        super(requestType);
    }

    public static ResolveHomeDatabaseStateTransition<BeginMessage> getInstanceForBegin() {
        return BEGIN_INSTANCE;
    }

    public static ResolveHomeDatabaseStateTransition<RunMessage> getInstanceForAutoCommit() {
        return RUN_INSTANCE;
    }

    @Override
    public StateReference process(Context ctx, M message, ResponseHandler handler) throws StateMachineException {
        if (message.impersonatedUser() == null && message.databaseName() == null) {
            ctx.connection().resolveDefaultDatabase();
        }

        return null;
    }
}
