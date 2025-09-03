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
package org.neo4j.bolt.protocol.common.fsm.transition.authentication;

import java.net.InetSocketAddress;
import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.error.StateMachineException;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.fsm.state.transition.AbstractStateTransition;
import org.neo4j.bolt.fsm.state.transition.StateTransition;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.authentication.AuthenticationMessage;
import org.neo4j.values.storable.Values;

/**
 * Handles propagate the advertised address to the client through response metadata.
 * <p />
 * This implementation was tailored to work combined with {@link AuthenticationStateTransition} from Bolt Protocol 5.8
 * and onward. The composition should be done by using the method {@link StateTransition#also(StateTransition[])} on
 * the transition object. Since this implementation doesn't return the next state.
 * <p />
 * Important to notice that this implementation doesn't make usage of the message. So this transition can used
 * in other states and messages. This is only located on this package for usage/documentation propose.
 */
public final class AppendAdvertisedAddressOnStateTransition extends AbstractStateTransition<AuthenticationMessage> {
    private static final AppendAdvertisedAddressOnStateTransition INSTANCE =
            new AppendAdvertisedAddressOnStateTransition();
    private static final String ADVERTISED_ADDRESS_KEY = "advertised_address";

    private AppendAdvertisedAddressOnStateTransition() {
        super(AuthenticationMessage.class);
    }

    public static AppendAdvertisedAddressOnStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    public StateReference process(Context ctx, AuthenticationMessage message, ResponseHandler handler)
            throws StateMachineException {
        var advertisedAddress = ctx.connection().connector().configuration().advertisedAddress();

        if (advertisedAddress != null) {
            var address = advertisedAddress.toString();
            if (advertisedAddress instanceof InetSocketAddress inetAddress) {
                address = String.format("%s:%d", inetAddress.getHostName(), inetAddress.getPort());
            }
            handler.onMetadata(ADVERTISED_ADDRESS_KEY, Values.stringValue(address));
        }

        // Do not move the state
        return null;
    }
}
