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

public class NodePropertySchemaInfoResult {
    /**
     * A combination of escaped label names interleaved by ":"
     */
    @Description("A name generated from the labels on the node.")
    public final String nodeType;

    /**
     * A list of label names
     */
    @Description("A list containing the labels on a category of node.")
    public final List<String> nodeLabels;
    /**
     * A property name that occurs on the given label combination or null
     */
    @Description("A property key on a category of node.")
    public final String propertyName;

    /**
     * A List containing all types of the given property on the given label combination or null
     */
    @Description("All types of a property belonging to a node category.")
    public final List<String> propertyTypes;

    /**
     * Indicates whether the property is present on all similar nodes (= true) or not (= false)
     */
    @Description("Whether or not the property is present on all nodes belonging to a node category.")
    public final boolean mandatory;

    public NodePropertySchemaInfoResult(
            String nodeType,
            List<String> nodeLabelsList,
            String propertyName,
            List<String> cypherTypes,
            boolean mandatory) {
        this.nodeType = nodeType;
        this.nodeLabels = nodeLabelsList;
        this.propertyName = propertyName;
        this.propertyTypes = cypherTypes;
        this.mandatory = mandatory;
    }
}
