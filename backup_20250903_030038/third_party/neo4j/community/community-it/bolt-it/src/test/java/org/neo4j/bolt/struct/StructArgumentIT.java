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
package org.neo4j.bolt.struct;

import java.io.IOException;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.initializer.EnableFeature;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class StructArgumentIT extends AbstractStructArgumentIT {

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenPoint2DIsSentWithInvalidCrsIdV40(@Authenticated BoltTestConnection connection)
            throws IOException {
        testFailureWithUnpackableValueV40(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.POINT_2D.getTag()))
                        .writeInt(5) // CRS
                        .writeFloat(3.15) // X
                        .writeFloat(4.012), // Y
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"5\"");
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenPoint2DIsSentWithInvalidCrsId(@Authenticated BoltTestConnection connection) throws IOException {

        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.POINT_2D.getTag()))
                        .writeInt(5) // CRS
                        .writeFloat(3.15) // X
                        .writeFloat(4.012), // Y
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"5\"",
                BoltConnectionAssertions.assertErrorCause(
                        "50N42: Unexpected error has occurred. See debug log for details.",
                        GqlStatusInfoCodes.STATUS_50N42.getGqlStatus(),
                        "error: general processing exception - unexpected error. Unexpected error has occurred. See debug log for details."));
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenPoint3DIsSentWithInvalidCrsIdV40(@Authenticated BoltTestConnection connection)
            throws IOException {
        testFailureWithUnpackableValueV40(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(4, StructType.POINT_3D.getTag()))
                        .writeInt(1200) // CRS
                        .writeFloat(3.15)
                        .writeFloat(4.012)
                        .writeFloat(5.905),
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"1200\"");
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenPoint3DIsSentWithInvalidCrsId(@Authenticated BoltTestConnection connection) throws IOException {

        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(4, StructType.POINT_3D.getTag()))
                        .writeInt(1200) // CRS
                        .writeFloat(3.15)
                        .writeFloat(4.012)
                        .writeFloat(5.905),
                "Illegal value for field \"params\": Illegal value for field \"crs\": Illegal coordinate reference system: \"1200\"",
                BoltConnectionAssertions.assertErrorCause(
                        "50N42: Unexpected error has occurred. See debug log for details.",
                        GqlStatusInfoCodes.STATUS_50N42.getGqlStatus(),
                        "error: general processing exception - unexpected error. Unexpected error has occurred. See debug log for details."));
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenPoint2DDimensionsDoNotMatchV40(@Authenticated BoltTestConnection connection) throws IOException {
        testFailureWithUnpackableValueV40(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.POINT_2D.getTag()))
                        .writeInt(CoordinateReferenceSystem.CARTESIAN_3D.getCode())
                        .writeFloat(3.15)
                        .writeFloat(4.012),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian-3d, x=3.15, y=4.012)");
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenPoint2DDimensionsDoNotMatch(@Authenticated BoltTestConnection connection) throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.POINT_2D.getTag()))
                        .writeInt(CoordinateReferenceSystem.CARTESIAN_3D.getCode())
                        .writeFloat(3.15)
                        .writeFloat(4.012),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian-3d, x=3.15, y=4.012)",
                BoltConnectionAssertions.assertErrorCauseWithInnerCause(
                        "08N06: General network protocol error.",
                        GqlStatusInfoCodes.STATUS_08N06.getGqlStatus(),
                        "error: connection exception - protocol error. General network protocol error.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                        BoltConnectionAssertions.assertErrorCauseWithInnerCause(
                                "22N24: Cannot construct a point from [3.15, 4.012].",
                                GqlStatusInfoCodes.STATUS_22N24.getGqlStatus(),
                                "error: data exception - invalid coordinate arguments. Cannot construct a point from [3.15, 4.012].",
                                BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                                BoltConnectionAssertions.assertErrorCause(
                                        "50N42: Unexpected error has occurred. See debug log for details.",
                                        GqlStatusInfoCodes.STATUS_50N42.getGqlStatus(),
                                        "error: general processing exception - unexpected error. Unexpected error has occurred. See debug log for details."))));
    }

    @ProtocolTest
    @IncludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenPoint3DDimensionsDoNotMatchV40(@Authenticated BoltTestConnection connection) throws IOException {
        testFailureWithUnpackableValueV40(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(4, StructType.POINT_3D.getTag()))
                        .writeInt(CoordinateReferenceSystem.CARTESIAN.getCode())
                        .writeFloat(3.15)
                        .writeFloat(4.012)
                        .writeFloat(5.905),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian, x=3.15, y=4.012, z=5.905)");
    }

    @ProtocolTest
    @ExcludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenPoint3DDimensionsDoNotMatch(@Authenticated BoltTestConnection connection) throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(4, StructType.POINT_3D.getTag()))
                        .writeInt(CoordinateReferenceSystem.CARTESIAN.getCode())
                        .writeFloat(3.15)
                        .writeFloat(4.012)
                        .writeFloat(5.905),
                "Illegal value for field \"params\": Illegal value for field \"coords\": Illegal CRS/coords combination (crs=cartesian, x=3.15, y=4.012, z=5.905)",
                BoltConnectionAssertions.assertErrorCauseWithInnerCause(
                        "08N06: General network protocol error.",
                        GqlStatusInfoCodes.STATUS_08N06.getGqlStatus(),
                        "error: connection exception - protocol error. General network protocol error.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                        BoltConnectionAssertions.assertErrorCauseWithInnerCause(
                                "22N24: Cannot construct a point from [3.15, 4.012, 5.905].",
                                GqlStatusInfoCodes.STATUS_22N24.getGqlStatus(),
                                "error: data exception - invalid coordinate arguments. Cannot construct a point from [3.15, 4.012, 5.905].",
                                BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                                BoltConnectionAssertions.assertErrorCause(
                                        "50N42: Unexpected error has occurred. See debug log for details.",
                                        GqlStatusInfoCodes.STATUS_50N42.getGqlStatus(),
                                        "error: general processing exception - unexpected error. Unexpected error has occurred. See debug log for details."))));
    }

    @ProtocolTest
    @EnableFeature(Feature.UTC_DATETIME)
    @IncludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenZonedDateTimeZoneIdIsNotKnownV40(@Authenticated BoltTestConnection connection)
            throws IOException {
        testFailureWithUnpackableValueV40(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.DATE_TIME_ZONE_ID.getTag()))
                        .writeInt(0)
                        .writeInt(0)
                        .writeString("Europe/Marmaris"),
                "Illegal value for field \"params\": Illegal value for field \"tz_id\": Illegal zone identifier: \"Europe/Marmaris\"");
    }

    @ProtocolTest
    @EnableFeature(Feature.UTC_DATETIME)
    @ExcludeWire({@Version(major = 5, minor = 6, range = 6), @Version(major = 4)})
    void shouldFailWhenZonedDateTimeZoneIdIsNotKnown(@Authenticated BoltTestConnection connection) throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.DATE_TIME_ZONE_ID.getTag()))
                        .writeInt(0)
                        .writeInt(0)
                        .writeString("Europe/Marmaris"),
                "Illegal value for field \"params\": Illegal value for field \"tz_id\": Illegal zone identifier: \"Europe/Marmaris\"",
                BoltConnectionAssertions.assertErrorCause(
                        "50N42: Unexpected error has occurred. See debug log for details.",
                        GqlStatusInfoCodes.STATUS_50N42.getGqlStatus(),
                        "error: general processing exception - unexpected error. Unexpected error has occurred. See debug log for details."));
    }
}
