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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFunSuite
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory

class SemanticGqlErrorTest extends SemanticFunSuite {

  test("update position in GQL object") {
    val se = SemanticError.invalidUseOfGraphFunction("function", InputPosition(44, 1, 45))

    // Check original positions
    val pos = se.gqlStatusObject.diagnosticRecord().get("_position").asInstanceOf[java.util.Map[String, Int]]
    pos.get("line") should be(1)
    pos.get("column") should be(45)
    pos.get("offset") should be(44)

    val exceptionFactory = Neo4jCypherExceptionFactory("query", Some(InputPosition(6, 1, 1)));
    val seAdjustedPosition = exceptionFactory.syntaxException(se.gqlStatusObject, "Message", se.position)

    // Check adjusted positions
    val adjustedPos =
      seAdjustedPosition.gqlStatusObject.diagnosticRecord().get("_position").asInstanceOf[java.util.Map[String, Int]]
    adjustedPos.get("line") should be(1)
    adjustedPos.get("column") should be(45)
    adjustedPos.get("offset") should be(50)
  }
}
