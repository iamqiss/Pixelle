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
import org.neo4j.cypher.internal.util.symbols.CTGraphRef
import org.neo4j.cypher.internal.util.symbols.CTString

object GraphByName extends Function {
  def name: String = "graph.byName"

  override val signatures = Vector(
    FunctionTypeSignature(
      function = this,
      names = Vector("name"),
      argumentTypes = Vector(CTString),
      outputType = CTGraphRef,
      description =
        "Returns the graph reference of the given name. It is only supported in the `USE` clause, on composite databases.",
      category = Category.GRAPH,
      argumentDescriptions = Map("name" -> "The name of the graph to be resolved.")
    )
  )
}
