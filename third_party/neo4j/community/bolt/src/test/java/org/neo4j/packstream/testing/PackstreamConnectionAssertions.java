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
package org.neo4j.packstream.testing;

import java.util.ArrayList;
import java.util.function.Predicate;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.neo4j.bolt.testing.assertions.BoltTestConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.error.BoltTestClientIOException;
import org.neo4j.bolt.testing.client.error.BoltTestClientInterruptedException;
import org.neo4j.packstream.io.PackstreamBuf;

public final class PackstreamConnectionAssertions
        extends BoltTestConnectionAssertions<PackstreamConnectionAssertions, BoltTestConnection> {
    private PackstreamConnectionAssertions(BoltTestConnection connection) {
        super(connection, PackstreamConnectionAssertions.class);
    }

    public static PackstreamConnectionAssertions assertThat(BoltTestConnection value) {
        return new PackstreamConnectionAssertions(value);
    }

    public static InstanceOfAssertFactory<BoltTestConnection, PackstreamConnectionAssertions> packstreamConnection() {
        return new InstanceOfAssertFactory<>(BoltTestConnection.class, PackstreamConnectionAssertions::new);
    }

    public PackstreamBufAssertions receivesMessage() {
        try {
            return PackstreamBufAssertions.assertThat(PackstreamBuf.wrap(this.actual.receiveMessage()));
        } catch (BoltTestClientIOException ex) {
            throw new AssertionError("Failed to retrieve expected message", ex);
        } catch (BoltTestClientInterruptedException ex) {
            throw new AssertionError("Interrupted while awaiting expected message", ex);
        }
    }

    public PackstreamBufListAssertions receivesMessages(Predicate<PackstreamBuf> assertions) {
        var accumulator = new ArrayList<PackstreamBuf>();

        try {
            PackstreamBuf buf;
            do {
                buf = PackstreamBuf.wrap(this.actual.receiveMessage());
                accumulator.add(buf);
            } while (assertions.test(buf));
        } catch (BoltTestClientIOException ex) {
            throw new AssertionError("Failed to retrieve expected message", ex);
        } catch (BoltTestClientInterruptedException ex) {
            throw new AssertionError("Interrupted while awaiting expected message", ex);
        }

        return new PackstreamBufListAssertions(accumulator);
    }
}
