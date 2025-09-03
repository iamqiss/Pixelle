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

import java.util.List;
import org.neo4j.procedure.Description;

public class RelationshipPropertySchemaInfoResult {
    /**
     * A relationship type
     */
    @Description("A name generated from the type on the relationship.")
    public final String relType;

    /**
     * A property name that occurs on the given relationship type or null
     */
    @Description("A property key on a category of relationship.")
    public final String propertyName;

    /**
     * A List containing all types of the given property on the given relationship type or null
     */
    @Description("All types of a property belonging to a relationship category.")
    public final List<String> propertyTypes;

    /**
     * Indicates whether the property is present on all similar relationships (= true) or not (= false)
     */
    @Description("Whether or not the property is present on all relationships belonging to a relationship category.")
    public final boolean mandatory;

    public RelationshipPropertySchemaInfoResult(
            String relType, String propertyName, List<String> cypherTypes, boolean mandatory) {
        this.relType = relType;
        this.propertyName = propertyName;
        this.propertyTypes = cypherTypes;
        this.mandatory = mandatory;
    }
}
