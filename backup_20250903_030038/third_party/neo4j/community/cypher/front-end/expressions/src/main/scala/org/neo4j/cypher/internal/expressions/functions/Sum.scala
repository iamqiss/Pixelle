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
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType

case object Sum extends AggregatingFunction {
  def name = "sum"

  override val signatures: Vector[FunctionTypeSignature] = Vector(
    FunctionTypeSignature(
      function = this,
      names = Vector("input"),
      argumentTypes = Vector(ClosedDynamicUnionType(Set(CTInteger, CTFloat, CTDuration))(InputPosition.NONE)),
      outputType = ClosedDynamicUnionType(Set(CTInteger, CTFloat, CTDuration))(InputPosition.NONE),
      description = "Returns the sum of a set of `INTEGER`, `FLOAT` or `DURATION` values",
      category = Category.AGGREGATING,
      argumentDescriptions = Map("input" -> "A value to be aggregated.")
    )
  )
}
