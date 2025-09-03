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
package org.neo4j.bolt.protocol.v52;

import org.neo4j.bolt.fsm.StateMachineConfiguration;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.AbstractBoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.protocol.common.fsm.transition.authentication.AuthenticationStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.authentication.LogoffStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.CreateAutocommitStatementStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.CreateTransactionStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.RouteStateTransition;
import org.neo4j.bolt.protocol.common.fsm.transition.ready.TelemetryStateTransition;
import org.neo4j.bolt.protocol.common.message.decoder.generic.TelemetryMessageDecoder;
import org.neo4j.bolt.protocol.common.message.encoder.FailureMessageEncoder;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.protocol.common.message.response.ResponseMessage;
import org.neo4j.bolt.protocol.v40.message.encoder.FailureMessageEncoderV40;
import org.neo4j.bolt.protocol.v44.fsm.response.metadata.MetadataHandlerV44;
import org.neo4j.bolt.protocol.v52.message.decoder.authentication.HelloMessageDecoderV52;
import org.neo4j.bolt.protocol.v52.message.decoder.transaction.BeginMessageDecoderV52;
import org.neo4j.bolt.protocol.v52.message.decoder.transaction.RunMessageDecoderV52;
import org.neo4j.packstream.struct.StructRegistry;

public final class BoltProtocolV52 extends AbstractBoltProtocol {
    private static final BoltProtocolV52 INSTANCE = new BoltProtocolV52();

    public static final ProtocolVersion VERSION = new ProtocolVersion(5, 2);

    public static BoltProtocolV52 getInstance() {
        return INSTANCE;
    }

    private BoltProtocolV52() {}

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    protected StructRegistry.Builder<Connection, RequestMessage> createRequestMessageRegistry() {
        return super.createRequestMessageRegistry()
                // Authentication
                .register(HelloMessageDecoderV52.getInstance())
                // Transaction
                .register(BeginMessageDecoderV52.getInstance())
                .register(RunMessageDecoderV52.getInstance())
                // Generic
                .unregister(TelemetryMessageDecoder.getInstance());
    }

    @Override
    protected StructRegistry.Builder<Connection, ResponseMessage> createResponseMessageRegistry() {
        return super.createResponseMessageRegistry()
                .unregister(FailureMessageEncoder.getInstance())
                .register(FailureMessageEncoderV40.getInstance());
    }

    @Override
    protected StateMachineConfiguration.Factory createStateMachine() {
        return super.createStateMachine()
                .withState(States.AUTHENTICATION, AuthenticationStateTransition.getInstance())
                .withState(
                        States.READY,
                        CreateTransactionStateTransition.getInstance(),
                        RouteStateTransition.getInstance(),
                        CreateAutocommitStatementStateTransition.getInstance(),
                        LogoffStateTransition.getInstance(),
                        TelemetryStateTransition.getInstance());
    }

    @Override
    public MetadataHandler metadataHandler() {
        return MetadataHandlerV44.getInstance();
    }
}
