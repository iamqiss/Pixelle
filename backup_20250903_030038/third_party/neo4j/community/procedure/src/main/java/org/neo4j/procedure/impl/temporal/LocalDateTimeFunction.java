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
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTLocalDateTime;

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
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

@Description("Creates a `LOCAL DATETIME` instant.")
class LocalDateTimeFunction extends TemporalFunction<LocalDateTimeValue> {
    private static final List<FieldSignature> INPUT_SIGNATURE = singletonList(
            inputField(
                    "input",
                    Neo4jTypes.NTAny,
                    DEFAULT_PARAMETER_VALUE,
                    false,
                    "Either a string representation of a temporal value, a map containing the single key 'timezone', or a map containing temporal values ('year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond', 'microsecond', 'nanosecond') as components."));

    LocalDateTimeFunction(Supplier<ZoneId> defaultZone) {
        super(NTLocalDateTime, INPUT_SIGNATURE, defaultZone);
    }

    @Override
    protected LocalDateTimeValue now(Clock clock, String timezone, Supplier<ZoneId> defaultZone) {
        return timezone == null ? LocalDateTimeValue.now(clock, defaultZone) : LocalDateTimeValue.now(clock, timezone);
    }

    @Override
    protected LocalDateTimeValue parse(TextValue value, Supplier<ZoneId> defaultZone) {
        return LocalDateTimeValue.parse(value);
    }

    @Override
    protected LocalDateTimeValue build(MapValue map, Supplier<ZoneId> defaultZone) {
        return LocalDateTimeValue.build(map, defaultZone);
    }

    @Override
    protected LocalDateTimeValue select(AnyValue from, Supplier<ZoneId> defaultZone) {
        return LocalDateTimeValue.select(from, defaultZone);
    }

    @Override
    protected List<FieldSignature> getTemporalTruncateSignature() {
        return Arrays.asList(
                inputField(
                        "unit",
                        Neo4jTypes.NTString,
                        "A string representing one of the following: 'microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'weekYear', 'quarter', 'year', 'decade', 'century', 'millennium'."),
                inputField(
                        "input",
                        Neo4jTypes.NTAny,
                        DEFAULT_PARAMETER_VALUE,
                        false,
                        "The date to be truncated using either `ZONED DATETIME`, `LOCAL DATETIME`, or `DATE`."),
                inputField(
                        "fields",
                        Neo4jTypes.NTMap,
                        nullValue(Neo4jTypes.NTMap),
                        false,
                        "A list of time components smaller than those specified in `unit` to preserve during truncation."));
    }

    @Override
    protected LocalDateTimeValue truncate(
            TemporalUnit unit, TemporalValue input, MapValue fields, Supplier<ZoneId> defaultZone) {
        return LocalDateTimeValue.truncate(unit, input, fields, defaultZone);
    }

    @Override
    protected String getTemporalCypherTypeName() {
        return "LOCAL DATETIME";
    }
}
