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
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTPoint

case object Point extends Function {
  override def name = "point"

  override val signatures = Vector(
    FunctionTypeSignature(
      this,
      names = Vector("input"),
      argumentTypes = Vector(CTMap),
      outputType = CTPoint,
      description =
        "Returns a 2D or 3D point object, given two or respectively three coordinate values in the Cartesian coordinate system or WGS 84 geographic coordinate system.",
      category = Category.SPATIAL,
      argumentDescriptions = Map(
        "input" -> "Cartesian 2D: {x :: FLOAT, y :: FLOAT, crs = \"cartesian\" :: STRING, srid = 7203 :: INTEGER}\n\nCartesian 3D: {x :: FLOAT, y :: FLOAT, z :: FLOAT, crs = \"cartesian-3D\" :: STRING, srid = 9157 :: INTEGER} \n\nWGS 84 2D: {longitude | x :: FLOAT, latitude | y :: FLOAT, crs = \"WGS-84-2D\" :: STRING, srid = 4326 :: INTEGER}\n\nWGS 84 3D: {longitude | x :: FLOAT, latitude | y :: FLOAT, height | z :: FLOAT, crs = \"WGS-84-3D\" :: STRING, srid = 4979 :: INTEGER}"
      )
    )
  )
}
