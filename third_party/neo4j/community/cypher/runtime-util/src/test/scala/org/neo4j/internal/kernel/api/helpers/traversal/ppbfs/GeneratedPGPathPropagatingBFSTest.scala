/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs

import org.neo4j.cypher.internal.logical.plans.TraversalMatchMode
import org.neo4j.cypher.internal.logical.plans.TraversalMatchMode.Trail
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GeneratedPGPathPropagatingBFSTest.testGraphs

class GeneratedPGPathPropagatingBFSTest extends CypherFunSuite with PGPathPropagatingBFSTestBase {

  for {
    nfa <- Seq(
      `(s) ((a)-->(b))* (t)`,
      `(s) ((a)-->(b))+ (t)`,
      `(s) ((a)--(b))+ (t)`,
      `(s) ((a)--(b)--(c))* (t)`,
      `(s) ((a)--(b)--(c))* (t) [single transition]`
    )
    graph <- testGraphs
    into <- Seq(true, false)
    grouped <- Seq(true, false)
    matchMode <- Seq(TraversalMatchMode.Trail, TraversalMatchMode.Walk)
    k <- if (matchMode == Trail) Seq(Int.MaxValue, 1, 2) else Seq(1, 2, 5)
  } {
    test(
      s"running the algorithm gives the same results as naive search. into=$into matchMode=$matchMode grouped=$grouped k=$k nfa=$nfa graph=${graph.render}"
    ) {
      var f = fixture()
        .withGraph(graph.graph)
        .from(graph.source)
        .withNfa(nfa)
        .withMatchMode(matchMode)

      if (matchMode == TraversalMatchMode.Walk) {
        f = f.withMaxDepth(3)
      }

      if (into) {
        f = f.into(graph.target)
      }

      if (grouped) {
        f = f.grouped
      }

      if (k != Int.MaxValue) {
        f = f.withK(k)
      }

      f.assertExpected()
    }
  }

}

object GeneratedPGPathPropagatingBFSTest {

  private case class NamedGraph(render: String, graph: TestGraph, source: Long, target: Long)

  /** a generated series of graphs consisting of a variable length chain of relationships from (start) to (end),
   * with another variable length chain connecting two nodes from the original */
  private lazy val testGraphs: Seq[NamedGraph] = {
    for {
      mainLength <- 1 to 4
      g2 = TestGraph.empty.line(mainLength)
      start = g2.nodes.min
      end = g2.nodes.max

      n1 <- g2.nodes
      n2 <- g2.nodes
      secondaryLength <- 1 to 3
      g3 = g2.chainRel(n1, n2, secondaryLength)
    } yield NamedGraph(
      s"(n$start)-$mainLength->(n$end), (n$n1)-$secondaryLength->(n$n2)",
      g3,
      start,
      end
    )
  }
}
