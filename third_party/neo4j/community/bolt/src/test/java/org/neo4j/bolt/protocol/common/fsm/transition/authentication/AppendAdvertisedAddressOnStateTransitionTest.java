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
import java.net.SocketAddress;
import java.net.UnixDomainSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.bolt.protocol.common.connector.Connector;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.transition.AbstractStateTransitionTest;
import org.neo4j.bolt.protocol.common.message.request.authentication.AuthenticationMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.HelloMessage;
import org.neo4j.bolt.protocol.common.message.request.authentication.LogonMessage;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.values.storable.Values;

class AppendAdvertisedAddressOnStateTransitionTest
        extends AbstractStateTransitionTest<AuthenticationMessage, AppendAdvertisedAddressOnStateTransition> {

    @Override
    protected AppendAdvertisedAddressOnStateTransition getTransition() {
        return AppendAdvertisedAddressOnStateTransition.getInstance();
    }

    private Stream<AuthenticationMessage> createRequests() {
        return Stream.of(
                        Map.<String, Object>of("scheme", "none"),
                        Map.<String, Object>of(
                                "scheme", "basic", "principal", "bob", "credentials", new byte[] {0x42, 0x21}))
                .flatMap(token -> Stream.of(
                        new HelloMessage(
                                "Test/1.0",
                                List.of(Feature.UTC_DATETIME),
                                new RoutingContext(false, Collections.emptyMap()),
                                token),
                        new LogonMessage(token)));
    }

    private Stream<AdvertisedAddressScenario> createAdvertisedAddressScenarios() {
        return this.createRequests()
                .flatMap(request -> Stream.of(
                        new AdvertisedAddressScenario(
                                InetSocketAddress.createUnresolved("localhost", 7687), "localhost:7687", request),
                        new AdvertisedAddressScenario(
                                InetSocketAddress.createUnresolved("my-db.neo4j.io", 7688),
                                "my-db.neo4j.io:7688",
                                request),
                        new AdvertisedAddressScenario(
                                new InetSocketAddress("localhost", 7687), "localhost:7687", request),
                        new AdvertisedAddressScenario(
                                new InetSocketAddress("my-db.neo4j.io", 7688), "my-db.neo4j.io:7688", request),
                        new AdvertisedAddressScenario(
                                UnixDomainSocketAddress.of("/var/my.sock"),
                                UnixDomainSocketAddress.of("/var/my.sock").toString(),
                                request)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReturnAdvertisedAddress() {
        return this.createAdvertisedAddressScenarios()
                .map(scenario -> DynamicTest.dynamicTest(scenario.toString(), () -> {
                    mockAdvertisedAddress(this.connection, scenario.address);

                    var targetState = this.transition.process(this.context, scenario.request, this.responseHandler);

                    Assertions.assertThat(targetState).isEqualTo(null);

                    var inOrder = Mockito.inOrder(this.context, this.connection, this.responseHandler);

                    inOrder.verify(this.context).connection();
                    inOrder.verify(this.responseHandler)
                            .onMetadata(
                                    Mockito.eq("advertised_address"),
                                    Mockito.eq(Values.stringValue(scenario.expectedAdvertisedAddress)));
                    inOrder.verify(this.context, Mockito.never()).defaultState(States.READY);
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldReturnNotAdvertisedAddressWhenNull() {
        return this.createRequests()
                .map(request -> DynamicTest.dynamicTest(request.toString(), () -> {
                    mockAdvertisedAddress(this.connection, null);

                    var targetState = this.transition.process(this.context, request, this.responseHandler);

                    Assertions.assertThat(targetState).isEqualTo(null);

                    var inOrder = Mockito.inOrder(this.context, this.connection, this.responseHandler);

                    inOrder.verify(this.context).connection();
                    inOrder.verify(this.connection, Mockito.never()).logon(request.authToken());
                    inOrder.verify(this.responseHandler, Mockito.never())
                            .onMetadata(Mockito.eq("advertised_address"), Mockito.any());
                    inOrder.verify(this.context, Mockito.never()).defaultState(States.READY);
                }));
    }

    private static void mockAdvertisedAddress(ConnectionHandle connection, SocketAddress socketAddress) {
        var connector = Mockito.mock(Connector.class);
        var configuration = Mockito.mock(Connector.Configuration.class);
        Mockito.doReturn(connector).when(connection).connector();
        Mockito.doReturn(configuration).when(connector).configuration();
        Mockito.doReturn(socketAddress).when(configuration).advertisedAddress();
    }

    private record AdvertisedAddressScenario(
            SocketAddress address, String expectedAdvertisedAddress, AuthenticationMessage request) {}
}
