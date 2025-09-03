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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.util.InputPosition
import org.scalatest.LoneElement

class ParserPositionTest extends AstParsingTestBase with LoneElement {

  test("MATCH (n) RETURN n.prop") {
    parses[Statements].withPositionOf[Property](InputPosition(17, 1, 18))
  }

  test("MATCH (n) SET n.prop = 1") {
    parses[Statements].withPositionOf[SetPropertyItem](InputPosition(14, 1, 15))
  }

  test("MATCH (n) REMOVE n.prop") {
    parses[Statements].withPositionOf[RemovePropertyItem](InputPosition(17, 1, 18))
  }

  test("LOAD CSV FROM 'url' AS line") {
    parses[Statements].withPositionOf[LoadCSV](InputPosition(0, 1, 1))
  }

  test("USE GRAPH(x) RETURN 1 as y ") {
    parses[Statements].withPositionOf[UseGraph](InputPosition(0, 1, 1))
  }

  test("CREATE (a)-[:X]->(b)") {
    parses[Statements].withPositionOf[NonPrefixedPatternPart](InputPosition(7, 1, 8))
  }

  test("SHOW ALL ROLES YIELD role") {
    parses[Statements].withPositionOf[Yield](InputPosition(15, 1, 16))
  }

  test("RETURN 3 IN list[0] AS r") {
    parses[Statements].withPositionOf[ContainerIndex](InputPosition(17, 1, 18))
  }

  test("RETURN 3 IN [1, 2, 3][0..1] AS r") {
    parses[Statements].withPositionOf[ListSlice](InputPosition(21, 1, 22))
  }

  test("MATCH (a) WHERE NOT (a:A)") {
    parses[Statements].withPositionOf[LabelExpressionPredicate](InputPosition(21, 1, 22))
  }

  test("MATCH (n) WHERE exists { (n) --> () }") {
    parses[Statements].withAstLike { ast =>
      val exists = ast.folder.treeFindByClass[ExistsExpression].get
      exists.position shouldBe InputPosition(16, 1, 17)
      exists.folder.treeFindByClass[Pattern].get.position shouldBe InputPosition(25, 1, 26)
    }
  }

  test("MATCH (n) WHERE exists { MATCH (n)-[r]->(m) }") {
    parses[Statements].withAstLike { ast =>
      val exists = ast.folder.treeFindByClass[ExistsExpression].get
      exists.position shouldBe InputPosition(16, 1, 17)
      exists.folder.treeFindByClass[Pattern].get.position shouldBe InputPosition(31, 1, 32)
    }
  }

  test("MATCH (n) WHERE exists { MATCH (m) WHERE exists { (n)-[]->(m) } }") {
    parses[Statements].withAstLike { ast: Statements =>
      ast.folder.findAllByClass[ExistsExpression] match {
        case Seq(exists, existsNested) =>
          exists.position shouldBe InputPosition(16, 1, 17)
          exists.folder.treeFindByClass[Pattern].get.position shouldBe InputPosition(31, 1, 32)
          existsNested.position shouldBe InputPosition(41, 1, 42)
          existsNested.folder.treeFindByClass[Pattern].get.position shouldBe InputPosition(50, 1, 51)
        case _ => fail("Expected existsExpressions to be a Seq of length 2")
      }
    }
  }

  test("MATCH (n) SET n += {name: null}") {
    parses[Statements].withPositionOf[SetIncludingPropertiesFromMapItem](InputPosition(14, 1, 15))
  }

  test("MATCH (n) SET n = {name: null}") {
    parses[Statements].withPositionOf[SetExactPropertiesFromMapItem](InputPosition(14, 1, 15))
  }

  Seq(
    ("DATABASES YIELD name", 21),
    ("DEFAULT DATABASE YIELD name", 28),
    ("HOME DATABASE YIELD name", 25),
    ("DATABASE $db YIELD name", 24),
    ("DATABASE neo4j YIELD name", 26)
  ).foreach { case (name, variableOffset) =>
    test(s"SHOW $name") {
      parses[Statements]
        .withPositionOf[ShowDatabase](InputPosition(0, 1, 1))
        .withAstLike { ast =>
          ast.folder.treeFind[Variable](_.name == "name").map(_.position) shouldBe
            Some(InputPosition(variableOffset, 1, variableOffset + 1))
        }
    }
  }

  test("CREATE INDEX FOR (p:Person) ON (p.name)") {
    parses[Statements]
      .withPositionOf[PropertyKeyName](InputPosition(34, 1, 35))
      .withPositionOf[LabelName](InputPosition(20, 1, 21))
  }
}
