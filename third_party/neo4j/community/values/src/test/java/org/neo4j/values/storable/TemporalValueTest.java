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
package org.neo4j.values.storable;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.DateTimeValue.datetime;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.neo4j.gqlstatus.ErrorGqlStatusObject;
import org.neo4j.values.virtual.MapValue;

public class TemporalValueTest {

    @Test
    void shouldTruncateNicely() {
        TemporalValue t = datetime(0, 1, 1, 14, 0, 3, 0, "UTC");
        TemporalUnit TU = ChronoUnit.WEEKS;
        MapValue fields = EMPTY_MAP;
        var zone = ZoneId.of("UTC");

        assertThatThrownBy(() -> LocalTimeValue.truncate(TU, t, fields, () -> zone))
                .asInstanceOf(InstanceOfAssertFactories.type(ErrorGqlStatusObject.class))
                .satisfies(e -> assertEquals(
                        e.cause().get().statusDescription(),
                        "error: data exception - invalid argument. Invalid argument: cannot process 'Weeks'."));
    }
}
