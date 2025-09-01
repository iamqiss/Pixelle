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
package org.neo4j.bolt.authentication;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.AbstractBoltWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
public class UserAgentIT {

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenUserAgentIsOmittedV40(@VersionSelected BoltTestConnection connection) throws IOException {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, AbstractBoltWire.MESSAGE_TAG_HELLO))
                .writeMap(Map.of("scheme", "none"))
                .getTarget());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(
                        Status.Request.Invalid,
                        "Illegal value for field \"user_agent\": Expected value to be non-null");
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenUserAgentIsOmitted(@VersionSelected BoltTestConnection connection) throws IOException {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, AbstractBoltWire.MESSAGE_TAG_HELLO))
                .writeMap(Map.of("scheme", "none"))
                .getTarget());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Request.Invalid,
                        "Illegal value for field \"user_agent\": Expected value to be non-null",
                        GqlStatusInfoCodes.STATUS_50N42.getGqlStatus(),
                        "error: general processing exception - unexpected error. Unexpected error has occurred. See debug log for details.");
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenInvalidUserAgentIsGivenV40(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello(x -> x.withScheme("none").withUserAgent(42L)));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(Status.Request.Invalid, "Illegal value for field \"user_agent\": Expected string");
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenInvalidUserAgentIsGiven(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello(x -> x.withScheme("none").withUserAgent(42L)));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureWithCause(
                        Status.Request.Invalid,
                        "Illegal value for field \"user_agent\": Expected string",
                        GqlStatusInfoCodes.STATUS_08N06.getGqlStatus(),
                        "error: connection exception - protocol error. General network protocol error.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                        BoltConnectionAssertions.assertErrorCauseWithInnerCause(
                                "22G03",
                                GqlStatusInfoCodes.STATUS_22G03.getGqlStatus(),
                                "error: data exception - invalid value type",
                                // 22G03 has UNKNOWN classification, no parameters and no position, so no diagnostic
                                // record is sent over Bolt.
                                // Instead a default diagnostic record is created on driver side.
                                null,
                                BoltConnectionAssertions.assertErrorCause(
                                        "",
                                        GqlStatusInfoCodes.STATUS_22N01.getGqlStatus(),
                                        "error: data exception - invalid type. Expected the value 42 to be of type STRING, but was of type Long.",
                                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord(
                                                "CLIENT_ERROR"))));
    }
}
