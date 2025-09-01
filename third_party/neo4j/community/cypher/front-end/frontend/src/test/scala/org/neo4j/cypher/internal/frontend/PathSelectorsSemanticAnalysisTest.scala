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
import org.neo4j.gqlstatus.GqlHelper

class PathSelectorsSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite {

  case class SelectorSyntax(
    syntax: String,
    selective: Boolean,
    shortest: Boolean
  )

  private val selectors = Seq(
    SelectorSyntax("ALL", selective = false, shortest = false),
    SelectorSyntax("ANY 2 PATHS", selective = true, shortest = false),
    SelectorSyntax("SHORTEST 2 PATHS", selective = true, shortest = true),
    SelectorSyntax("ALL SHORTEST PATHS", selective = true, shortest = true),
    SelectorSyntax("SHORTEST 1 PATH GROUPS", selective = true, shortest = true)
  )

  private val allSelectiveSelectors = selectors.filter(_.selective).map(_.syntax)

  test(s"MATCH path = ((a)-->(b))+ RETURN count(*)") {
    val result = runSemanticAnalysis()
    result.errorMessages shouldBe empty
  }

  // Selectors may be placed inside QPPs and PPPs if separated by a subquery expression
  Seq(
    "r IN COLLECT",
    "EXISTS",
    "r.prop = COUNT"
  ).foreach { operation =>
    selectors.map(_.syntax).foreach { selector =>
      test(s"MATCH ((a)-[r]-(b WHERE $operation { MATCH $selector ((c)-[q]-(d))+ RETURN q } ))+ RETURN 1") {
        runSemanticAnalysis().errorMessages shouldBe empty
      }
    }
  }

  // A path pattern with a selective selector may not have path patterns beside it in the same graph pattern.
  allSelectiveSelectors.foreach { selector =>
    test(s"""MATCH
            |   p1 = $selector (a)-->*(c)-->(c),
            |   p2 = (x)-->*(c)-->(z)
            |RETURN count(*)""".stripMargin) {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        "Multiple path patterns cannot be used in the same clause in combination with a selective path selector."
      )
    }
    test(s"""MATCH
            |   p2 = (x)-->*(c)-->(z),
            |   p1 = $selector (a)-->*(c)-->(c)
            |RETURN count(*)""".stripMargin) {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        "Multiple path patterns cannot be used in the same clause in combination with a selective path selector."
      )
    }
  }

  // ... but for non-selective, this does not hold.
  Seq("", "ALL").foreach { firstSelector =>
    Seq("", "ALL").foreach { secondSelector =>
      test(s"""MATCH
              |   p2 = $firstSelector (x)-->*(c)-->(z),
              |   p1 = $secondSelector (a)-->*(c)-->(c)
              |RETURN count(*)""".stripMargin) {
        runSemanticAnalysis().errorMessages shouldBe empty
      }
    }
  }

  Seq(
    "shortestPath",
    "allShortestPaths"
  ).foreach { shortest =>
    test(
      s"""MATCH
         |   p1 = (a)-->(c)-->(c),
         |   p2 = $shortest((x)-[*]->(c))
         |RETURN count(*)""".stripMargin
    ) {
      runSemanticAnalysis().errorMessages shouldBe empty
    }
  }

  // Should allow more than one QPP
  selectors.filter(_.shortest).map(_.syntax).foreach { selector =>
    test(s"MATCH $selector ((a)-[r]->(b))+ ((c)-[s]->(d))+ RETURN count(*)") {
      runSemanticAnalysis().errorMessages shouldBe empty
    }
  }

  (selectors.filter(!_.shortest).map(_.syntax) :+ "").foreach { selector =>
    test(s"MATCH $selector ((a)-[r]->(b))+ ((c)-[s]->(d))+ RETURN count(*)") {
      runSemanticAnalysis().errorMessages shouldBe empty
    }
  }

  // Selectors with disallowed counts

  Seq(
    ("ANY 0", "path"),
    ("ANY 0 PATH", "path"),
    ("ANY 0 PATHS", "path"),
    ("SHORTEST 0", "path"),
    ("SHORTEST 0 PATH", "path"),
    ("SHORTEST 0 PATHS", "path"),
    ("SHORTEST 0 GROUP", "group"),
    ("SHORTEST 0 GROUPS", "group"),
    ("SHORTEST 0 PATH GROUP", "group"),
    ("SHORTEST 0 PATH GROUPS", "group"),
    ("SHORTEST 0 PATHS GROUP", "group"),
    ("SHORTEST 0 PATHS GROUPS", "group")
  ).foreach { case (selector, kind) =>
    test(s"MATCH $selector ((a)-[]->(b))+ RETURN a") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        s"The $kind count needs to be greater than 0."
      )
    }
  }

  // Too large selectors

  Seq(
    "ANY 9999999999999999999999999999999999999999999",
    "ANY 9999999999999999999999999999999999999999999 PATH",
    "ANY 9999999999999999999999999999999999999999999 PATHS",
    "SHORTEST 9999999999999999999999999999999999999999999",
    "SHORTEST 9999999999999999999999999999999999999999999 PATH",
    "SHORTEST 9999999999999999999999999999999999999999999 PATHS",
    "SHORTEST 9999999999999999999999999999999999999999999 GROUP",
    "SHORTEST 9999999999999999999999999999999999999999999 GROUPS",
    "SHORTEST 9999999999999999999999999999999999999999999 PATH GROUP",
    "SHORTEST 9999999999999999999999999999999999999999999 PATH GROUPS",
    "SHORTEST 9999999999999999999999999999999999999999999 PATHS GROUP",
    "SHORTEST 9999999999999999999999999999999999999999999 PATHS GROUPS"
  ).foreach { selector =>
    test(s"MATCH $selector ((a)-[]->(b))+ RETURN a") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        s"integer is too large"
      )
    }
  }

  test(s"MATCH SHORTEST 2 PATH GROUPS ((a)-[r]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldBe Seq.empty
  }

  // WHERE clauses in Parenthesized Path Patterns

  selectors.foreach { selector =>
    test(s"MATCH path = ${selector.syntax} ((a)-[r]->+(b) WHERE a.prop = b.prop) RETURN 1") {
      val result = runSemanticAnalysis()
      result.errorMessages shouldBe empty
    }
  }

  // Do semantic checking in the WHERE clause
  selectors.foreach { selector =>
    test(s"MATCH ${selector.syntax} ((a) WHERE c.prop) RETURN 1") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        "Variable `c` not defined"
      )
    }
  }

  test(s"MATCH ALL (path = (a)-[r]->+(b)<-[s]-+(c) WHERE length(path) > 3) RETURN path") {
    runSemanticAnalysis().error shouldBe SemanticError(
      GqlHelper.getGql42001_42N42(1, 12, 11),
      "Sub-path assignment is currently not supported.",
      InputPosition(11, 1, 12)
    )
  }

  test(s"MATCH p = (q = (a)-[r]->+(b)<-[s]-+(c) WHERE length(q) > 3) RETURN p, q") {
    runSemanticAnalysis().error shouldBe SemanticError(
      GqlHelper.getGql42001_42N42(1, 12, 11),
      "Sub-path assignment is currently not supported.",
      InputPosition(11, 1, 12)
    )
  }

  selectors.map(_.syntax).foreach { selector =>
    test(s"MATCH p = $selector ((a)-[r]->+(b)<-[s]-+(c) WHERE length(p) > 3) RETURN p") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        """From within a parenthesized path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
          |In this case, `p` is defined in the same `MATCH` clause as ((a) (()-[r]->())+ (b) (()<-[s]-())+ (c) WHERE length(p) > 3).""".stripMargin
      )
    }
  }

  allSelectiveSelectors.foreach { selector =>
    test(s"MATCH $selector (path = (a)-[r]->+(b)<-[s]-+(c) WHERE length(path) > 3) RETURN path") {
      runSemanticAnalysis().errorMessages shouldBe empty
    }
  }

  // Mixing selective selectors with shortestPath/allShortestPaths is not allowed
  allSelectiveSelectors.foreach { selector =>
    test(s"MATCH $selector shortestPath((a)-->(b)) RETURN *") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
      )
    }

    test(s"MATCH $selector allShortestPaths((a)-->(b)) RETURN *") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
      )
    }

    test(s"MATCH $selector (a)-->(b) WHERE shortestPath((a)-->(b)) IS NOT NULL RETURN *") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
      )
    }

    test(s"MATCH $selector (a)-->(b) WHERE EXISTS { MATCH shortestPath((a)-->(b)) } RETURN *") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
      )
    }

    test(s"CALL { MATCH $selector (a)-->(b) MATCH shortestPath((c)-->(d)) RETURN * } RETURN *") {
      runSemanticAnalysis().errorMessages shouldBe Seq(
        "Mixing shortestPath/allShortestPaths with path selectors (e.g. 'ANY SHORTEST') or explicit match modes ('e.g. DIFFERENT RELATIONSHIPS') is not allowed."
      )
    }

    test(s"MATCH $selector (a)-->(b) MATCH shortestPath((c)-->(d)) RETURN *") {
      runSemanticAnalysis().errorMessages shouldBe empty
    }
  }

  test(s"MATCH ALL shortestPath((a)-->(b)) RETURN *") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(s"MATCH ALL allShortestPaths((a)-->(b)) RETURN *") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(s"MATCH ALL (a)-->(b) WHERE shortestPath((a)-->(b)) IS NOT NULL RETURN *") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(s"MATCH ALL (a)-->(b) WHERE EXISTS { MATCH shortestPath((a)-->(b)) } RETURN *") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(s"CALL { MATCH ALL (a)-->(b) MATCH shortestPath((c)-->(d)) RETURN * } RETURN *") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(s"MATCH ALL (a)-->(b) MATCH shortestPath((c)-->(d)) RETURN *") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }
}
