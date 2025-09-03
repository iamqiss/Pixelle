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

case object Log10 extends Function {
  def name = "log10"

  override val signatures = Vector(
    FunctionTypeSignature(
      this,
      names = Vector("input"),
      argumentTypes = Vector(CTFloat),
      outputType = CTFloat,
      description = "Returns the common logarithm (base 10) of a `FLOAT`.",
      category = Category.LOGARITHMIC,
      argumentDescriptions = Map("input" -> "A value for which the common logarithm (base 10) will be returned.")
    )
  )
}
