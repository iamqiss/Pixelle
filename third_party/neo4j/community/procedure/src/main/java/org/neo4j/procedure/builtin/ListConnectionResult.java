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
package org.neo4j.procedure.builtin;

import java.time.ZoneId;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.procedure.Description;

public class ListConnectionResult {
    @Description("The id of the connection.")
    public final String connectionId;

    @Description("The time the connection was established, formatted according to the ISO-8601 Standard.")
    public final String connectTime;

    @Description("The protocol of the connector.")
    public final String connector;

    @Description("The username of the connected user.")
    public final String username;

    @Description("The active agent.")
    public final String userAgent;

    @Description("The address of the connected server.")
    public final String serverAddress;

    @Description("The address of the connected client.")
    public final String clientAddress;

    ListConnectionResult(TrackedNetworkConnection connection, ZoneId timeZone) {
        connectionId = connection.id();
        connectTime = ProceduresTimeFormatHelper.formatTime(connection.connectTime(), timeZone);
        connector = connection.connectorId();
        username = connection.username();
        userAgent = connection.userAgent();
        serverAddress = SocketAddress.format(connection.serverAddress());
        clientAddress = SocketAddress.format(connection.clientAddress());
    }
}
