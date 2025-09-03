/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.expressions.functions

import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTPoint

case object Distance extends Function {
  val name = "point.distance"

  override val signatures = Vector(
    FunctionTypeSignature(
      function = this,
      names = Vector("from", "to"),
      argumentTypes = Vector(CTPoint, CTPoint),
      outputType = CTFloat,
      description =
        "Returns a `FLOAT` representing the distance between any two points in the same CRS. If the points are in the WGS 84 CRS, the function returns the geodesic distance (i.e., the shortest path along the curved surface of the Earth). If the points are in a Cartesian CRS, the function returns the Euclidean distance (i.e., the shortest straight-line distance in a flat, planar space).",
      category = Category.SPATIAL,
      argumentDescriptions = Map("from" -> "A start point.", "to" -> "An end point in the same CRS as the start point.")
    )
  )
}
