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
package org.neo4j.bolt.test.extension.resolver.connection;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.neo4j.bolt.test.extension.lifecycle.TransportConnectionManager;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;

public class ConnectionProviderParameterResolver extends AbstractConnectionInitializingParameterResolver {

    public ConnectionProviderParameterResolver(
            TransportConnectionManager connectionManager, BoltWire wire, TransportType transportType) {
        super(connectionManager, wire, transportType);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType().isAssignableFrom(ConnectionProvider.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return (ConnectionProvider) () -> this.acquireConnection(extensionContext, parameterContext);
    }
}
