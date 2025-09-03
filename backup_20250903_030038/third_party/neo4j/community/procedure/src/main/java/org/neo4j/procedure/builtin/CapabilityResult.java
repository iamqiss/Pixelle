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

import org.neo4j.capabilities.Capability;
import org.neo4j.procedure.Description;

public class CapabilityResult {
    @Description("The full name of the capability (e.g. \"dbms.instance.version\").")
    public final String name;

    @Description("The capability description (e.g. \"Neo4j version this instance is running\").")
    public final String description;

    @Description("The capability object if it is present in the system (e.g. \"5.20.0\").")
    public final Object value;

    public CapabilityResult(Capability<?> capability, Object value) {
        this.name = capability.name().fullName();
        this.description = capability.description();
        this.value = value;
    }
}
