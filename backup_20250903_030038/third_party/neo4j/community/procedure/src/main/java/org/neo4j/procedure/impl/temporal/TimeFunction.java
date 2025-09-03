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
package org.neo4j.procedure.impl.temporal;

import static java.util.Collections.singletonList;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTTime;

import java.time.Clock;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.neo4j.internal.kernel.api.procs.FieldSignature;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.procedure.Description;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.virtual.MapValue;

@Description("Creates a `ZONED TIME` instant.")
class TimeFunction extends TemporalFunction<TimeValue> {

    private static final List<FieldSignature> INPUT_SIGNATURE = singletonList(
            inputField(
                    "input",
                    Neo4jTypes.NTAny,
                    DEFAULT_PARAMETER_VALUE,
                    false,
                    "Either a string representation of a temporal value, a map containing the single key 'timezone', or a map containing temporal values ('hour', 'minute', 'second', 'millisecond', 'microsecond', 'nanosecond', 'timezone') as components."));

    TimeFunction(Supplier<ZoneId> defaultZone) {
        super(NTTime, INPUT_SIGNATURE, defaultZone);
    }

    @Override
    protected TimeValue now(Clock clock, String timezone, Supplier<ZoneId> defaultZone) {
        return timezone == null ? TimeValue.now(clock, defaultZone) : TimeValue.now(clock, timezone);
    }

    @Override
    protected TimeValue parse(TextValue value, Supplier<ZoneId> defaultZone) {
        return TimeValue.parse(value, defaultZone);
    }

    @Override
    protected TimeValue build(MapValue map, Supplier<ZoneId> defaultZone) {
        return TimeValue.build(map, defaultZone);
    }

    @Override
    protected TimeValue select(AnyValue from, Supplier<ZoneId> defaultZone) {
        return TimeValue.select(from, defaultZone);
    }

    @Override
    protected List<FieldSignature> getTemporalTruncateSignature() {
        return Arrays.asList(
                inputField(
                        "unit",
                        Neo4jTypes.NTString,
                        "A string representing one of the following: 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day'."),
                inputField(
                        "input",
                        Neo4jTypes.NTAny,
                        DEFAULT_PARAMETER_VALUE,
                        false,
                        "The date to be truncated using either `ZONED DATETIME`, `LOCAL DATETIME`, `ZONED TIME`, or `LOCAL TIME`."),
                inputField(
                        "fields",
                        Neo4jTypes.NTMap,
                        nullValue(Neo4jTypes.NTMap),
                        false,
                        "A list of time components smaller than those specified in `unit` to preserve during truncation."));
    }

    @Override
    protected TimeValue truncate(
            TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone) {
        return TimeValue.truncate(unit, input, fields, defaultZone);
    }

    @Override
    protected String getTemporalCypherTypeName() {
        return "ZONED TIME";
    }
}
