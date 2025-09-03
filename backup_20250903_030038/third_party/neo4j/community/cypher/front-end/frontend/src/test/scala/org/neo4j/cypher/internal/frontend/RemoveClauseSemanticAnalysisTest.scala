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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.GqlParams

class RemoveClauseSemanticAnalysisTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  test("MATCH (n) REMOVE n[\"prop\"]") {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("MATCH (n), (m) REMOVE (CASE WHEN n.prop = 5 THEN n ELSE m END)[\"prop\"]") {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("MATCH (n), (m) REMOVE n[1]") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError.invalidEntityType(
        "Integer",
        "node or relationship property key",
        List("String"),
        "Type mismatch: node or relationship property key must be given as String, but was Integer",
        InputPosition(24, 1, 25)
      )
    )
  }

  test("MATCH (n)-[r]->(m) REMOVE r[5.0]") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError.invalidEntityType(
        "Float",
        "node or relationship property key",
        List("String"),
        "Type mismatch: node or relationship property key must be given as String, but was Float",
        InputPosition(28, 1, 29)
      )
    )
  }

  test("WITH 5 AS var MATCH (n) REMOVE n[var]") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError.invalidEntityType(
        "Integer",
        "node or relationship property key",
        List("String"),
        "Type mismatch: node or relationship property key must be given as String, but was Integer",
        InputPosition(33, 1, 34)
      )
    )
  }

  test("WITH {key: 1} AS var REMOVE var['key']") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError.invalidEntityType(
        "Map",
        GqlParams.StringParam.ident.process("var"),
        List("Node", "Relationship"),
        "Type mismatch: expected Node or Relationship but was Map",
        InputPosition(28, 1, 29)
      )
    )
  }

  test("MATCH (n) REMOVE n[\"prop2\"]") {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("MATCH ()-[r]->() REMOVE r[\"prop2\"]") {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("MATCH (n) REMOVE n.prop") {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("MATCH (n) REMOVE n IS Label") {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }

  test("MATCH (n) REMOVE n :Label") {
    runSemanticAnalysis().errors.toSet shouldBe empty
  }
}
