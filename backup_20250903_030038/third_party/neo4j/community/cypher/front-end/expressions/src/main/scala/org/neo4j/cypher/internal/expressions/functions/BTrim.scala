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
import org.neo4j.cypher.internal.util.symbols.CTString

case object BTrim extends Function {
  def name = "btrim"

  override val signatures = Vector(
    FunctionTypeSignature(
      function = this,
      names = Vector("input"),
      argumentTypes = Vector(CTString),
      outputType = CTString,
      description = "Returns the given `STRING` with leading and trailing whitespace removed.",
      category = Category.STRING,
      argumentDescriptions = Map("input" -> "A value from which all leading and trailing whitespace will be removed.")
    ),
    FunctionTypeSignature(
      function = this,
      names = Vector("input", "trimCharacterString"),
      argumentTypes = Vector(CTString, CTString),
      outputType = CTString,
      description = "Returns the given `STRING` with leading and trailing `trimCharacterString` characters removed.",
      category = Category.STRING,
      argumentDescriptions = Map(
        "input" -> "A value from which the leading and trailing trim character will be removed.",
        "trimCharacterString" -> "A character to be removed from the start and end of the given string."
      )
    )
  )
}
