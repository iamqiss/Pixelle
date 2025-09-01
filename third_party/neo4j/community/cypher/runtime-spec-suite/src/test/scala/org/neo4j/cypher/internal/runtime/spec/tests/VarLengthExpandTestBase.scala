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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.TraversalMatchMode
import org.neo4j.cypher.internal.runtime.InputValues
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.RelationshipType
import org.scalatest.Outcome

import java.lang.System.lineSeparator

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Random

abstract class VarLengthExpandTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int,
  protected val traversalMatchMode: TraversalMatchMode
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("simple var-length-expand") {
    // given
    val n = sizeHint / 6
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array(path.startNode, path.take(length).endNode())

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("simple var-length-expand, including start node") {
    // given
    val n = sizeHint / 6
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*0..]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("var-length-expand with bound relationships") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield {
        val pathPrefix = path.take(length)
        Array[Object](pathPrefix.startNode, pathPrefix.relationships(), pathPrefix.endNode())
      }

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand with bound relationships, including start node") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*0..]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield {
        val pathPrefix = path.take(length)
        Array[Object](pathPrefix.startNode, pathPrefix.relationships(), pathPrefix.endNode())
      }

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand on lollipop graph") {
    // given
    val (Seq(n1, n2, n3), Seq(r1, r2, r3)) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1, Array(r1), n2),
        Array(n1, Array(r1, r3), n3),
        Array(n1, Array(r2), n2),
        Array(n1, Array(r2, r3), n3)
      )

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand on lollipop graph, including start node") {
    // given
    val (Seq(n1, n2, n3), Seq(r1, r2, r3)) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*0..]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1, Array.empty, n1),
        Array(n1, Array(r1), n2),
        Array(n1, Array(r1, r3), n3),
        Array(n1, Array(r2), n2),
        Array(n1, Array(r2, r3), n3)
      )

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand with max length") {
    // given
    val (Seq(n1, n2, _), Seq(r1, r2, _)) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*..1]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1, Array(r1), n2),
        Array(n1, Array(r2), n2)
      )

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand with max length, including start node") {
    // given
    val (Seq(n1, n2, _), Seq(r1, r2, _)) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*0..1]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      Array(
        Array(n1, Array.empty, n1),
        Array(n1, Array(r1), n2),
        Array(n1, Array(r2), n2)
      )

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand with min and max length") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*2..4]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 2 to 4
      } yield {
        val pathPrefix = path.take(length)
        Array[Object](pathPrefix.startNode, pathPrefix.relationships(), pathPrefix.endNode())
      }

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand with length 0") {
    // given
    val (nodes, _) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[r*0]->(y)", matchMode = traversalMatchMode)
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  // VAR EXPAND INTO

  test("simple var-length-expand-into") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected: IndexedSeq[Array[Node]] = paths.map(p => Array(p.startNode, p.endNode()))

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("var-length-expand-into with non-matching end") {
    // given
    val (paths, nonMatching) = givenGraph {
      val paths = chainGraphs(1, "TO", "TO", "TO", "TOO", "TO")
      val nonMatching = nodeGraph(1).head
      (paths, nonMatching)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val input = inputValues(Array(paths.head.startNode, nonMatching))
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("var-length-expand-into with end not being a node") {
    // given
    val paths = givenGraph { chainGraphs(1, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(variables = Seq("x", "y"))
      .build()

    val input = inputValues(Array(paths.head.startNode, 42))
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("var-length-expand-into with end bound to mix of matching and non-matching nodes") {
    // given
    val n = closestMultipleOf(sizeHint / 5, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }
    val random = new Random

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    case class InputDef(start: Node, end: Node, matches: Boolean)
    val input =
      for (i <- 0 until n) yield {
        val matches = random.nextBoolean()
        val endNode =
          if (matches) {
            paths(i).endNode()
          } else {
            val not_i = (i + 1 + random.nextInt(paths.size - 1)) % n
            paths(not_i).endNode()
          }
        InputDef(paths(i).startNode, endNode, matches)
      }

    val runtimeResult = execute(logicalQuery, runtime, inputColumns(4, n / 4, i => input(i).start, i => input(i).end))

    // then
    val expected: IndexedSeq[Array[Node]] =
      input.filter(_.matches).map(p => Array(p.start, p.end))

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("var-length-expand-into on lollipop graph") {
    // given
    val (Seq(n1, _, n3), Seq(r1, r2, r3)) = givenGraph { lollipopGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)", expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(n1, n3)))

    // then
    val expected =
      Array(
        Array(n1, Array(r1, r3), n3),
        Array(n1, Array(r2, r3), n3)
      )

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand-into with max length") {
    // given
    val (n1, n3, r4) = givenGraph {
      val (Seq(n1, _, n3), _) = lollipopGraph()
      val r4 = n1.createRelationshipTo(n3, RelationshipType.withName("R"))
      (n1, n3, r4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*..1]->(y)", expandMode = ExpandInto)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(n1, n3)))

    // then
    val expected = Array(Array(n1, Array(r4), n3))

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("var-length-expand-into with min length") {
    // given
    val (n1, n3, r1, r2, r3, r4) = givenGraph {
      val (Seq(n1, _, n3), Seq(r1, r2, r3)) = lollipopGraph()
      val r4 = tx.getNodeById(n1.getId).createRelationshipTo(tx.getNodeById(n3.getId), RelationshipType.withName("R"))
      (n1, n3, r1, r2, r3, r4)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*2..2]->(y)", expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(n1, n3)))

    // then
    val expected = Array(
      Array(n1, Array(r1, r3), n3),
      Array(n1, Array(r2, r3), n3)
    )

    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  // PATH PROJECTION

  test("should project (x)-[r*]->(y) correctly when from matching from x") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)", projectedDir = OUTGOING, expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array[Object](p.startNode, p.relationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (x)-[r*]->(y) correctly when from matching from y") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(y)<-[r*]-(x)", projectedDir = OUTGOING, expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array[Object](p.startNode, p.relationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (y)<-[r*]-(x) correctly when from matching from x") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())): _*)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]->(y)", projectedDir = INCOMING, expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array[Object](p.startNode, p.reverseRelationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (y)<-[r*]-(x) correctly when from matching from y") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())): _*)

    val pattern = traversalMatchMode match {
      case TraversalMatchMode.Walk  => "(y)<-[r*..10]-(x)"
      case TraversalMatchMode.Trail => "(y)<-[r*]-(x)"
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand(pattern, projectedDir = INCOMING, expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array[Object](p.startNode, p.reverseRelationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (x)-[r*]-(y) correctly when from matching from x") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())): _*)
    val pattern = traversalMatchMode match {
      case TraversalMatchMode.Walk  => "(x)-[r*..5]-(y)"
      case TraversalMatchMode.Trail => "(x)-[r*]-(y)"
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand(pattern, projectedDir = OUTGOING, expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = paths.map(p => Array[Object](p.startNode, p.relationships(), p.endNode()))
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  test("should project (x)-[r*]-(y) correctly when from matching from y") {
    // given
    val paths = givenGraph { chainGraphs(3, "TO", "TO", "TO", "TOO", "TO") }
    val input = inputValues(paths.map(p => Array[Any](p.startNode, p.endNode())): _*)
    val pattern = traversalMatchMode match {
      case TraversalMatchMode.Walk  => "(y)-[r*..5]-(x)"
      case TraversalMatchMode.Trail => "(y)-[r*]-(x)"
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand(pattern, projectedDir = INCOMING, expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        paths.map(p => Array[Object](p.startNode, p.relationships(), p.endNode()))
      case TraversalMatchMode.Trail =>
        paths.map(p => Array[Object](p.startNode, p.relationships(), p.endNode()))
    }
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }

  // NULL INPUT

  test("should handle null from-node") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]-(y)", matchMode = traversalMatchMode)
      .input(nodes = Seq("x"))
      .build()

    val input = inputValues(Array(Array[Any](null)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  test("should handle null from-node without overwriting to-node in expand into") {
    // given
    val n1 = givenGraph { nodeGraph(1).head }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand("(x)-[r*]-(y)", expandMode = ExpandInto, matchMode = traversalMatchMode)
      .input(nodes = Seq("x", "y"))
      .build()

    val input = inputValues(Array(Array[Any](null, n1)): _*)
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    runtimeResult should beColumns("x", "r", "y").withNoRows()
  }

  // EXPANSION FILTERING, DIRECTION

  test("should filter on outgoing direction") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r*1..2]->(y)")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sb1),
      Array(g.sa1),
      Array(g.middle),
      Array(g.sb2),
      Array(g.middle),
      Array(g.sc3),
      Array(g.ea1),
      Array(g.eb1),
      Array(g.ec1)
    ))
  }

  test("should filter on incoming direction") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)<-[r*1..2]-(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sc1),
      Array(g.sc2)
    ))
  }

  test("should expand on BOTH direction") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r*1..2]-(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        Array(
          // First step to (sa1)
          Array(g.sa1),
          Array(g.start),
          Array(g.middle),
          // First step to (sb1)
          Array(g.sb1),
          Array(g.sb2),
          Array(g.start),
          // First step to (sc1)
          Array(g.sc1),
          Array(g.sc2),
          Array(g.start),
          // First step to (middle)
          Array(g.middle),
          Array(g.end),
          Array(g.sa1),
          Array(g.sb2),
          Array(g.sc3),
          Array(g.start),
          Array(g.ea1),
          Array(g.eb1),
          Array(g.ec1)
        )
      case TraversalMatchMode.Trail =>
        Array(
          Array(g.sb1), // outgoing only
          Array(g.sa1),
          Array(g.middle),
          Array(g.sb2),
          Array(g.middle),
          Array(g.sc3),
          Array(g.ea1),
          Array(g.eb1),
          Array(g.ec1),
          Array(g.sc1), // incoming only
          Array(g.sc2),
          Array(g.sb2), // mixed
          Array(g.sa1),
          Array(g.end)
        )
    }
    runtimeResult should beColumns("y").withRows(expected)
  }

  // EXPANSION FILTERING, RELATIONSHIP TYPE

  test("should filter on relationship type A") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r:A*1..2]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.middle),
      Array(g.middle),
      Array(g.sc3),
      Array(g.ea1),
      Array(g.ec1)
    ))
  }

  test("should filter on relationship type B") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand("(x)-[r:B*1..2]->(y)", matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sb1),
      Array(g.sb2)
    ))
  }

  // EXPANSION FILTERING, NODE AND RELATIONSHIP PREDICATE

  test("should filter on node predicate") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand(
        "(x)-[r:*1..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.middle.getId)),
        matchMode = traversalMatchMode
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        Array(
          Array(g.sa1),
          Array(g.start),
          Array(g.sb1),
          Array(g.sb2),
          Array(g.start),
          Array(g.sc1),
          Array(g.sc2),
          Array(g.start)
        )
      case TraversalMatchMode.Trail =>
        Array(
          Array(g.sa1),
          Array(g.sb1),
          Array(g.sb2),
          Array(g.sc1),
          Array(g.sc2)
        )
    }
    // then
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should filter on two node predicates") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand(
        "(x)-[r:*1..3]-(y)",
        nodePredicates = Seq(
          Predicate("n", "id(n) <> " + g.middle.getId),
          Predicate("n2", "id(n2) <> " + g.sc3.getId)
        ),
        matchMode = traversalMatchMode
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        Array(
          // first step to (sa1)
          Array(g.sa1), // (start)-->(sa1)
          Array(g.start), // (start)-->(sa1)-->(start)
          Array(g.sa1), // (start)-->(sa1)-->(start)-->(sa1)
          Array(g.sb1), // (start)-->(sa1)-->(start)-->(sb1)
          Array(g.sc1), // (start)-->(sa1)-->(start)-->(sc1)
          // first step to (sb1)
          Array(g.sb1), // (start)-->(sb1)
          Array(g.sb2), // (start)-->(sb1)-->(sb2)
          Array(g.sb1), // (start)-->(sa1)-->(sb2)-->(sb1)
          Array(g.start), // (start)-->(sb1)-->(start)
          Array(g.sa1), // (start)-->(sb1)-->(start)-->(sa1)
          Array(g.sb1), // (start)-->(sb1)-->(start)-->(sb1)
          Array(g.sc1), // (start)-->(sb1)-->(start)-->(sc1)
          // first step to (sc1)
          Array(g.sc1), // (start)-->(sc1)
          Array(g.sc2), // (start)-->(sc1)-->(sc2)
          Array(g.sc1), // (start)-->(sc1)-->(sc2)-->(sc1)
          Array(g.start), // (start)-->(sc1)-->(start)
          Array(g.sa1), // (start)-->(sc1)-->(start)-->(sa1)
          Array(g.sb1), // (start)-->(sc1)-->(start)-->(sb1)
          Array(g.sc1) // (start)-->(sc1)-->(start)-->(sc1)
        )
      case TraversalMatchMode.Trail =>
        Array(
          Array(g.sa1),
          Array(g.sb1),
          Array(g.sb2),
          Array(g.sc1),
          Array(g.sc2)
        )
    }
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should filter on node predicate on first node") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand(
        "(x)-[r:*1..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.start.getId)),
        matchMode = traversalMatchMode
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on node predicate on first node from reference") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand(
        "(X)-[r:*1..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.start.getId)),
        matchMode = traversalMatchMode
      )
      .projection("x AS X")
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withNoRows()
  }

  test("should filter on relationship predicate") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand(
        "(x)-[r:*1..2]->(y)",
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId)),
        matchMode = traversalMatchMode
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("y").withRows(Array(
      Array(g.sa1),
      Array(g.middle),
      Array(g.sb1),
      Array(g.sb2)
    ))
  }

  test("should filter on two relationship predicates") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand(
        "(x)-[r:*1..3]-(y)",
        relationshipPredicates = Seq(
          Predicate("r", "id(r) <> " + g.startMiddle.getId),
          Predicate("r2", "id(r2) <> " + g.endMiddle.getId)
        ),
        matchMode = traversalMatchMode
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    val expected = traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        Array(
          // first step to (sa1)
          Array(g.sa1), // (start)-->(sa1)
          Array(g.start), // (start)-->(sa1)-->(start)
          Array(g.sa1), // (start)-->(sa1)-->(start)-->(sa1)
          Array(g.sb1), // (start)-->(sa1)-->(start)-->(sb1)
          Array(g.sc1), // (start)-->(sa1)-->(start)-->(sc1)
          Array(g.middle), // (start)-->(sa1)-->(middle)
          Array(g.ea1), // (start)-->(sa1)-->(middle)-->(ea1)
          Array(g.eb1), // (start)-->(sa1)-->(middle)-->(eb1)
          Array(g.ec1), // (start)-->(sa1)-->(middle)-->(ec1)
          Array(g.sa1), // (start)-->(sa1)-->(middle)-->(sa1)
          Array(g.sb2), // (start)-->(sa1)-->(middle)-->(sb2)
          Array(g.sc3), // (start)-->(sa1)-->(middle)-->(sc3)
          // first step to (sb1)
          Array(g.sb1), // (start)-->(sb1)
          Array(g.sb2), // (start)-->(sb1)-->(sb2)
          Array(g.sb1), // (start)-->(sb1)-->(sb2)-->(sb1)
          Array(g.middle), // (start)-->(sb1)-->(sb2)-->(middle)
          Array(g.start), // (start)-->(sb1)-->(start)
          Array(g.sa1), // (start)-->(sb1)-->(start)-->(sa1)
          Array(g.sb1), // (start)-->(sb1)-->(start)-->(sb1)
          Array(g.sc1), // (start)-->(sb1)-->(start)-->(sc1)
          // first step to (sc1)
          Array(g.sc1), // (start)-->(sc1)
          Array(g.sc2), // (start)-->(sc1)-->(sc2)
          Array(g.sc1), // (start)-->(sc1)-->(sc2)-->(sc1)
          Array(g.sc3), // (start)-->(sc1)-->(sc2)-->(sc3)
          Array(g.start), // (start)-->(sc1)-->(start)
          Array(g.sa1), // (start)-->(sc1)-->(start)-->(sa1)
          Array(g.sb1), // (start)-->(sc1)-->(start)-->(sb1)
          Array(g.sc1) // (start)-->(sc1)-->(start)-->(sc1)
        )
      case TraversalMatchMode.Trail =>
        Array(
          Array(g.sa1),
          Array(g.sb1),
          Array(g.sc1),
          Array(g.middle),
          Array(g.sb2),
          Array(g.sc2),
          Array(g.ea1),
          Array(g.eb1),
          Array(g.ec1),
          Array(g.sc3),
          Array(g.sb2),
          Array(g.middle),
          Array(g.sc3)
        )
    }

    // then
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should filter on node and relationship predicate") {
    // given
    val g = givenGraph { sineGraph() }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .expand(
        "(x)-[r:*2..2]-(y)",
        nodePredicates = Seq(Predicate("n", "id(n) <> " + g.sa1.getId)),
        relationshipPredicates = Seq(Predicate("r", "id(r) <> " + g.startMiddle.getId)),
        matchMode = traversalMatchMode
      )
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then

    val expected = traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        Array(
          Array(g.sc2),
          Array(g.start),
          Array(g.sb2),
          Array(g.start)
        )
      case TraversalMatchMode.Trail =>
        Array(
          Array(g.sc2),
          Array(g.sb2)
        )
    }
    runtimeResult should beColumns("y").withRows(expected)
  }

  test("should handle predicate accessing start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand(
        "(x)-[*]->(y)",
        nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")),
        matchMode = traversalMatchMode
      )
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing start node, including start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand(
        "(x)-[*0..]->(y)",
        nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")),
        matchMode = traversalMatchMode
      )
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand into with predicate accessing end node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand(
        "(x)-[*]->(y)",
        expandMode = ExpandInto,
        nodePredicates = Seq(Predicate("n", "'END' IN labels(y)")),
        matchMode = traversalMatchMode
      )
      .input(nodes = Seq("x", "y"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected: IndexedSeq[Array[Node]] = paths.map(p => Array(p.startNode, p.endNode()))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing start node when reference") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand(
        "(x)-[*]->(y)",
        nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")),
        matchMode = traversalMatchMode
      )
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing start node when reference and including start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand(
        "(x)-[*0..]->(y)",
        nodePredicates = Seq(Predicate("n", "'START' IN labels(x)")),
        matchMode = traversalMatchMode
      )
      .input(variables = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle expand into with predicate accessing end node when reference") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand(
        "(x)-[*]->(y)",
        expandMode = ExpandInto,
        nodePredicates = Seq(Predicate("n", "'END' IN labels(y)")),
        matchMode = traversalMatchMode
      )
      .input(variables = Seq("x", "y"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected: IndexedSeq[Array[Node]] = paths.map(p => Array(p.startNode, p.endNode()))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing reference in context") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", nodePredicates = Seq(Predicate("n", "id(n) >= zero")), matchMode = traversalMatchMode)
      .projection("0 AS zero")
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing reference in context and including start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*0..]->(y)", nodePredicates = Seq(Predicate("n", "id(n) >= zero")), matchMode = traversalMatchMode)
      .projection("0 AS zero")
      .input(nodes = Seq("x"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing node in context") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*]->(y)", nodePredicates = Seq(Predicate("n", "id(other) >= 0")), matchMode = traversalMatchMode)
      .projection("0 AS zero")
      .input(nodes = Seq("x", "other"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle predicate accessing node in context including start node") {
    // given
    val n = closestMultipleOf(10, 4)
    val paths = givenGraph { chainGraphs(n, "TO", "TO", "TO", "TOO", "TO") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*0..]->(y)", nodePredicates = Seq(Predicate("n", "id(other) >= 0")), matchMode = traversalMatchMode)
      .projection("0 AS zero")
      .input(nodes = Seq("x", "other"))
      .build()

    val input = inputColumns(4, n / 4, i => paths(i).startNode, i => paths(i).endNode())
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 5
      } yield Array(path.startNode, path.take(length).endNode())
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle var expand + predicate on cached property") {
    // given
    val n = sizeHint / 6
    val paths = givenGraph {
      val ps = chainGraphs(n, "TO", "TO", "TO", "TOO", "TO")
      // set incrementing node property values along chain
      for {
        p <- ps
        i <- 0 until p.length()
        n = p.nodeAt(i)
      } n.setProperty("prop", i)
      // set property of last node to lowest value, so VarLength predicate fails
      for {
        p <- ps
        n = p.nodeAt(p.length())
      } n.setProperty("prop", -1)
      ps
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b", "c")
      .expand(
        "(b)-[*]->(c)",
        nodePredicates = Seq(Predicate("n", "n.prop > cache[a.prop]")),
        matchMode = traversalMatchMode
      )
      .expandAll("(a)-[:TO]->(b)")
      .nodeByLabelScan("a", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 1 to 3
        p = path.slice(1, 1 + length)
      } yield Array(p.startNode, p.endNode())

    runtimeResult should beColumns("b", "c").withRows(expected)
  }

  test("should handle var expand + predicate on cached property + including start node") {
    // given
    val n = sizeHint / 6
    val paths = givenGraph {
      val ps = chainGraphs(n, "TO", "TO", "TO", "TOO", "TO")
      // set incrementing node property values along chain
      for {
        p <- ps
        i <- 0 until p.length()
        n = p.nodeAt(i)
      } n.setProperty("prop", i)
      // set property of last node to lowest value, so VarLength predicate fails
      for {
        p <- ps
        n = p.nodeAt(p.length())
      } n.setProperty("prop", -1)
      ps
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b", "c")
      .expand(
        "(b)-[*0..]->(c)",
        nodePredicates = Seq(Predicate("n", "n.prop > cache[a.prop]")),
        matchMode = traversalMatchMode
      )
      .expandAll("(a)-[:TO]->(b)")
      .nodeByLabelScan("a", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      for {
        path <- paths
        length <- 0 to 3
        p = path.slice(1, 1 + length)
      } yield Array(p.startNode, p.endNode())

    runtimeResult should beColumns("b", "c").withRows(expected)
  }

  test("var length expand on long paths") {
    // given
    val pathLength = 150 // We're interested in triggering special behaviour in pipelined for long paths
    val paths = givenGraph {

      /*                    TO      TO
       *                 /------>*----->* .... long path
       *                /      /
       *  SuperStart   *------- ALSO_TO
       *                \
       *                 \----->*----->* ... long path
       *                   TO      TO
       */
      val Seq(branch1, branch2) = chainGraphs(2, (1 to pathLength).map(_ => "TO"): _*)
      val start = runtimeTestSupport.tx.createNode(Label.label("SuperStart"))
      val rel1 = start.createRelationshipTo(branch1.startNode, RelationshipType.withName("TO"))
      val rel2 = start.createRelationshipTo(branch1.startNode, RelationshipType.withName("ALSO_TO"))
      val rel3 = start.createRelationshipTo(branch2.startNode, RelationshipType.withName("TO"))

      Seq(
        (start, Seq(rel1) ++ branch1.relationships().asScala, branch1.endNode()),
        (start, Seq(rel2) ++ branch1.relationships().asScala, branch1.endNode()),
        (start, Seq(rel3) ++ branch2.relationships().asScala, branch2.endNode())
      )
    }

    val input = new InputValues()
      .and(Array(paths.head._1, paths.head._3))
      .and(Array(paths.head._1, paths.last._3))

    val pattern = traversalMatchMode match {
      case TraversalMatchMode.Walk  => s"(x)-[r*..${pathLength + 1}]->(y)"
      case TraversalMatchMode.Trail => "(x)-[r*]->(y)"
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand(pattern, ExpandInto, matchMode = traversalMatchMode)
      .input(Seq("x", "y"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, input)

    val expected = for {
      (startNode, rels, endNode) <- paths
    } yield {
      Array[Object](startNode, rels.asJava, endNode)
    }

    runtimeResult should beColumns("x", "r", "y").withRows(expected, listInAnyOrder = true)

  }

  test("var length expand on long partially doubly linked list") {
    // given
    val nodeSize = 128 // We're interested in triggering special behaviour in pipelined for long paths
    val backwardRelCount = 10
    val (forwardRelationships, backwardsRelationships) = givenGraph {
      /*
       *       FORWARD       FORWARD
       *        ----->       ----->
       * START *      *  ...        * END
       *                     <-----
       *                     BACKWARD (backwardRelCount number of BACKWARDS relations)
       */
      val backType = RelationshipType.withName("BACKWARDS")
      val forwardChain = chainGraphs(1, (1 until nodeSize).map(_ => "FORWARD"): _*).head
      val backRels = forwardChain.relationships().asScala
        .zipWithIndex
        .map {
          case (forwardRel, index) if index >= nodeSize - backwardRelCount - 1 =>
            Some(forwardRel.getEndNode.createRelationshipTo(forwardRel.getStartNode, backType))
          case _ => None
        }
        .toIndexedSeq
      (forwardChain.relationships().asScala.toIndexedSeq, backRels)
    }
    val pattern = traversalMatchMode match {
      case TraversalMatchMode.Walk  => "(x)-[r*1..128]->(y)"
      case TraversalMatchMode.Trail => "(x)-[r*1..]->(y)"
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .expand(pattern, matchMode = traversalMatchMode)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val fromNode = forwardRelationships.head.getStartNode
    val nodes = fromNode +: forwardRelationships.map(_.getEndNode)
    val firstNodeIndexWithBack = nodeSize - backwardRelCount
    traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        runtimeResult should beColumns("x", "r", "y").withRows(rowCount(1103))
      case TraversalMatchMode.Trail =>
        val expected = for {
          turnPointNodeIndex <- 1 until nodeSize
          toNodeIndex <- turnPointNodeIndex to math.min(firstNodeIndexWithBack - 1, turnPointNodeIndex) by -1
        } yield {
          val backRels = backwardsRelationships.slice(toNodeIndex, turnPointNodeIndex).map(_.get).reverse
          val rels = forwardRelationships.take(turnPointNodeIndex) ++ backRels
          val toNode = nodes(toNodeIndex)
          Array[Object](fromNode, rels.asJava, toNode)
        }
        runtimeResult should beColumns("x", "r", "y").withRows(expected, listInAnyOrder = true)
    }
  }

  test("high cardinality fuse-able var length expand followed by expand") {
    // given
    val depth = 3
    val outDegree = 4
    givenGraph {
      circleGraph(nNodes = sizeHint, relType = "R", outDegree)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b", "c")
      .expand("(b)-[:R]->(c)")
      .filter("id(b) >= 0") // this is only here to make the var-length expand fuse-able
      .expand(s"(a)-[:R*$depth..$depth]->(b)", matchMode = traversalMatchMode)
      .allNodeScan("a")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedRowCount = sizeHint * Math.pow(outDegree, depth).asInstanceOf[Int] * outDegree
    runtimeResult should beColumns("a", "b", "c").withRows(rowCount(expectedRowCount))
  }

  test(
    "TraversalEndpoint(To) should resolve as the next node of a relationship traversal during predicate evaluation"
  ) {
    val (a, b, c) = givenGraph {
      val graph = fromTemplate("""
        (c:TO)-->(a)-->(b:TO)
                  |
                  v
              (ignored)
       """)
      (graph node "a", graph node "b", graph node "c")
    }

    val relPredicates = Seq(
      VariablePredicate(varFor("r"), hasLabels(TraversalEndpoint(varFor("temp"), Endpoint.To), "TO"))
    )

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("t")
      .expandExpr("(s)-[r*]-(t)", relationshipPredicates = relPredicates, matchMode = traversalMatchMode)
      .nodeByIdSeek("s", Set.empty, a.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("t").withRows(inAnyOrder(Seq(Array(b), Array(c))))
  }

  test(
    "TraversalEndpoint(From) should resolve as the previous node of a relationship traversal during predicate evaluation"
  ) {
    val (a, b, c) = givenGraph {
      val graph = fromTemplate("""
        (a:FROM)<--(b:FROM)-->(c)-->()
       """)
      (graph node "a", graph node "b", graph node "c")
    }

    val relPredicates = Seq(
      VariablePredicate(varFor("r"), hasLabels(TraversalEndpoint(varFor("temp"), Endpoint.From), "FROM"))
    )
    val pattern = traversalMatchMode match {
      case TraversalMatchMode.Walk  => "(s)-[r*..3]-(t)"
      case TraversalMatchMode.Trail => "(s)-[r*]-(t)"
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("s", "t")
      .expandExpr(pattern, relationshipPredicates = relPredicates, matchMode = traversalMatchMode)
      .nodeByIdSeek("s", Set.empty, a.getId)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    val expected = traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        inAnyOrder(Seq(
          Array(a, b),
          Array(a, a),
          Array(a, b),
          Array(a, c)
        ))
      case TraversalMatchMode.Trail =>
        inAnyOrder(Seq(
          Array(a, b),
          Array(a, c)
        ))
    }

    runtimeResult should beColumns("s", "t").withRows(expected)
  }

  // HELPERS

  private def closestMultipleOf(sizeHint: Int, div: Int) = (sizeHint / div) * div
}

abstract class PipelinedVarLengthExpandTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int,
  traversalMatchMode: TraversalMatchMode,
  varExpandRelationshipIdSetThreshold: Int = Random.nextInt(128) - 1
) extends VarLengthExpandTestBase[CONTEXT](
      edition.copyWith(GraphDatabaseInternalSettings.var_expand_relationship_id_set_threshold -> Int.box(
        varExpandRelationshipIdSetThreshold
      )),
      runtime,
      sizeHint,
      traversalMatchMode
    ) {

  override def withFixture(test: NoArgTest): Outcome = {
    withClue(s"Failed with varExpandRelationshipIdSetThreshold=$varExpandRelationshipIdSetThreshold${lineSeparator()}")(
      super.withFixture(test)
    )
  }
}
