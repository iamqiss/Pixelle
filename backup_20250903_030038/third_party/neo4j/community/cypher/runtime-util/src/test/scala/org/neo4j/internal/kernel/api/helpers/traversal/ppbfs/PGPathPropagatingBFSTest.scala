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
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.function.Predicates
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTest._
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.AddTarget
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.CursorNextRelationship
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.CursorSetNode
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.Expand
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.ExpandNode
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.NextLevel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.EventRecorder.ReturnPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.MultiRelationshipExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.MultiRelationshipExpansion.CompoundPredicate
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder.MultiRelationshipBuilder.r
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipPredicate
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.LocalMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.scalatest.Inspectors

import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala

class PGPathPropagatingBFSTest extends CypherFunSuite with PGPathPropagatingBFSTestBase {

  /*************************************
   * simple tests on the nfa traversal *
   *************************************/

  test("single node is yielded when state is both start and final state") {
    val graph = TestGraph.builder
    val n = graph.node()

    val paths = fixture()
      .withGraph(graph.build())
      .from(n)
      .withNfa { sb =>
        sb.newState(isStartState = true, isFinalState = true)
      }
      .paths()

    paths shouldBe Seq(Seq(n))
  }

  test("single node is yielded when there is an unconditional node juxtaposition between start and final state") {
    val graph = TestGraph.builder
    val n = graph.node()

    val paths = fixture()
      .withGraph(graph.build())
      .from(n)
      .withNfa { sb =>
        sb.newState(isStartState = true).addNodeJuxtaposition(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(n, n))
  }

  test("single node is omitted when there is an unmet conditional node juxtaposition between start and final state") {
    val graph = TestGraph.builder
    val n = graph.node()

    val paths = fixture()
      .withGraph(graph.build())
      .from(n)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true, predicate = Predicates.ALWAYS_FALSE_LONG)
        sb.newState(isStartState = true).addNodeJuxtaposition(e)
      }
      .paths()

    paths shouldBe Seq.empty
  }

  test(
    "single node with loop is yielded when there is an unconditional relationship expansion between start and final state"
  ) {
    val graph = TestGraph.builder
    val n = graph.node()
    val r = graph.rel(n, n)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(n, r, n))
  }

  test(
    "two nodes and their relationship are yielded when there is an unconditional relationship expansion between start and final state"
  ) {
    val graph = `(n1)-->(n2)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n1, graph.r1_2, graph.n2))
  }

  test(
    "relationship expansions can traverse a relationship in the inverse direction"
  ) {
    val graph = `(n1)-->(n2)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n2)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n2, graph.r1_2, graph.n1))
  }

  test("relationship expansions can traverse a loop") {
    val graph = TestGraph.builder
    val a = graph.node()
    val r = graph.rel(a, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(
          sb.newState(isFinalState = true),
          direction = Direction.OUTGOING
        )
      }
      .from(a)
      .paths()

    paths shouldBe Seq(Seq(a, r, a))
  }

  test("multiple discrete paths between two paths can be yielded") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val r1 = graph.rel(n0, n1)
    val r2 = graph.rel(n0, n1)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths should contain theSameElementsAs Seq(
      Seq(n0, r1, n1),
      Seq(n0, r2, n1)
    )
  }

  test("relationship expansions may be filtered on the destination node") {
    val graph = `(a)<--(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        sb.newState(isStartState = true)
          .addRelationshipExpansion(sb.newState(isFinalState = true, predicate = (n: Long) => n == graph.a))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.ba, graph.a))
  }

  test("relationship expansions may be filtered on the relationship id") {
    val graph = `(a)<--(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, RelationshipPredicate.onId(_ == graph.bc))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.bc, graph.c))
  }

  test("relationship expansions may be filtered on the relationship type") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n0, n1, 99)
    val r = graph.rel(n0, n2, 66)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, RelationshipPredicate.onType(_ == 66))
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may expose an array of accepted types") {
    val graph = TestGraph.builder
    val n0 = graph.node()
    val n1 = graph.node()
    val n2 = graph.node()
    graph.rel(n0, n1, 99)
    val r = graph.rel(n0, n2, 66)

    val paths = fixture()
      .withGraph(graph.build())
      .from(n0)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, types = Array(66))
      }
      .paths()

    paths shouldBe Seq(Seq(n0, r, n2))
  }

  test("relationship expansions may be filtered on INCOMING direction") {
    val graph = `(n1)-->(n2)-->(n3)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n2)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, direction = Direction.INCOMING)
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n2, graph.r1_2, graph.n1))
  }

  test("relationship expansions may be filtered on OUTGOING direction") {
    val graph = `(n1)-->(n2)-->(n3)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n2)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, direction = Direction.OUTGOING)
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n2, graph.r2_3, graph.n3))
  }

  test("relationship expansions may be filtered on the source node of the relationship") {
    val graph = `(a)-->(b)<--(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, RelationshipPredicate.onSource(_ == graph.a))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.ab, graph.a))
  }

  test("relationship expansions may be filtered on the target node of the relationship") {
    val graph = `(a)<--(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa { sb =>
        val e = sb.newState(isFinalState = true)
        sb.newState(isStartState = true).addRelationshipExpansion(e, RelationshipPredicate.onTarget(_ == graph.c))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.bc, graph.c))
  }

  test("two-hop path") {
    val graph = `(n1)-->(n2)-->(n3)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa { sb =>
        val s = sb.newState()
        sb.newState(isStartState = true).addRelationshipExpansion(s)
        s.addRelationshipExpansion(sb.newState(isFinalState = true))
      }
      .paths()

    paths shouldBe Seq(Seq(graph.n1, graph.r1_2, graph.n2, graph.r2_3, graph.n3))
  }

  /********************************
   * More complex NFAs and graphs *
   ********************************/

  test("paths of different lengths are yielded in order of length") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val cd = graph.rel(c, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(anyDirectedPath)
      .paths()

    paths shouldBe Seq(
      Seq(a),
      Seq(a, ab, b),
      Seq(a, ab, b, bc, c),
      Seq(a, ab, b, bc, c, cd, d)
    )
  }

  test("relationships are not traversed twice in the same direction") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)
    val ba = graph.rel(b, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, a, ab, b, b),
      Seq(a, a, ab, b, b, ba, a, a)
    )
  }

  test("relationships are not traversed again in the opposite direction") {
    val graph = `(n1)-->(n2)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(`(s) ((a)--(b))+ (t)`)
      .paths()

    paths shouldBe Seq(
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2)
    )
  }

  test("loops are traversed in any order") {
    val graph = TestGraph.builder
    val a = graph.node()

    val r1 = graph.rel(a, a)
    val r2 = graph.rel(a, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(`(s) ((a)-->(b))+ (t)`)
      .paths()

    paths should contain theSameElementsAs Seq(
      Seq(a, a, r1, a, a),
      Seq(a, a, r2, a, a),
      Seq(a, a, r1, a, a, r2, a, a),
      Seq(a, a, r2, a, a, r1, a, a)
    )
  }

  test("an r* pattern should be able to be represented by a two NFA states with NJ then loop RE") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val cd = graph.rel(c, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val e = sb.newState("e", isFinalState = true)
        s.addNodeJuxtaposition(e)
        e.addRelationshipExpansion(e)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, a),
      Seq(a, a, ab, b),
      Seq(a, a, ab, b, bc, c),
      Seq(a, a, ab, b, bc, c, cd, d)
    )
  }

  test("an r+ pattern should be able to be represented by two NFA states with an RE and loop RE") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val cd = graph.rel(c, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val e = sb.newState("e", isFinalState = true)

        s.addRelationshipExpansion(e)
        e.addRelationshipExpansion(e)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, ab, b),
      Seq(a, ab, b, bc, c),
      Seq(a, ab, b, bc, c, cd, d)
    )
  }

  test("an r+ pattern should be able to be represented by three NFA states with an initial NJ") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val cd = graph.rel(c, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val e = sb.newState("e", isFinalState = true)

        s.addNodeJuxtaposition(a)
        a.addRelationshipExpansion(e)
        e.addNodeJuxtaposition(a)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, a, ab, b),
      Seq(a, a, ab, b, b, bc, c),
      Seq(a, a, ab, b, b, bc, c, c, cd, d)
    )
  }

  test("multiple relationship expansion yields path without duplicate relationship") {
    val graph = `(a)-->(b)<--(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.a)
      .withNfa(`(s) ((a)-->(b)<--(c))+ (t)`)
      .paths()

    paths shouldBe Seq(
      Seq(graph.a, graph.a, graph.ab, graph.b, graph.cb, graph.c, graph.c)
    )
  }

  test("multiple relationship expansion works with bidirectional search") {
    val graph = `(n1)-->(n2)<--(n3)-->(n4)<--(n5)-->(n6)<--(n7)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n3)
      .into(graph.n7)
      .withNfa(`(s) ((a)-->(b)<--(c))+ (t)`)
      .paths()

    paths shouldBe Seq(
      Seq(
        graph.n3,
        graph.n3,
        graph.r3_4,
        graph.n4,
        graph.r5_4,
        graph.n5,
        graph.n5,
        graph.r5_6,
        graph.n6,
        graph.r7_6,
        graph.n7,
        graph.n7
      )
    )
  }

  /**************************************************************
   * grouping, K limit, into (early exit), filtering, max depth *
   **************************************************************/

  test("results are limited to K") {
    val graph = `(a)<--(b)-->(a)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa(singleRelPath)
      .withK(1)
      .paths()

    paths should (be(Seq(Seq(graph.b, graph.ba1, graph.a)))
      or be(Seq(Seq(graph.b, graph.ba2, graph.a))))
  }

  test("results are limited to K per source-target pair") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val ab1 = graph.rel(a, b)
    val ab2 = graph.rel(a, b)
    val ac1 = graph.rel(a, c)
    val ac2 = graph.rel(a, c)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(singleRelPath)
      .withK(1)
      .paths()

    paths should have length 2
    paths should (contain(Seq(a, ab1, b)) or contain(Seq(a, ab2, b)))
    paths should (contain(Seq(a, ac1, c)) or contain(Seq(a, ac2, c)))
  }

  test("results are limited to K groups when grouping is enabled") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    graph.rel(b, c)
    val cd = graph.rel(c, d)

    val ac = graph.rel(a, c)
    val bd = graph.rel(b, d)

    val ad = graph.rel(a, d)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val start = sb.newState(isStartState = true)
        val a = sb.newState()
        val end = sb.newState(isFinalState = true, predicate = (n: Long) => n == d)
        start.addRelationshipExpansion(a, direction = Direction.OUTGOING)
        a.addNodeJuxtaposition(start)
        a.addNodeJuxtaposition(end)
      }
      .grouped
      .withK(2)
      .paths()

    paths should contain theSameElementsAs Seq(
      Seq(a, ad, d, d),
      Seq(a, ac, c, c, cd, d, d),
      Seq(a, ab, b, b, bd, d, d)
    )
  }

  test(
    "results are limited to K groups when grouping is enabled and the final element of a group is rejected from the filter"
  ) {
    // this tests a very specific interaction in the ppbfs iterator - see https://trello.com/c/G4MEDqwE/
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    graph.rel(a, b)
    graph.rel(b, c)
    graph.rel(c, d)

    graph.rel(a, c)
    graph.rel(b, d)

    graph.rel(a, d)

    val onlyFirstOfGroup = {
      var passed = Set.empty[Int]
      (length: Int) =>
        if (!passed.contains(length)) {
          passed = passed + length
          true
        } else false
    }

    val lengths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa(anyDirectedPath)
      .project(TracedPath.fromSignpostStack(_).length)
      .filter(onlyFirstOfGroup)
      .into(d, SearchMode.Unidirectional)
      .grouped
      .withK(2)
      .toList

    // prior to the bugfix, this would have yielded Seq(1, 2, 3)
    lengths shouldBe Seq(1, 2)
  }

  test("intoTarget does not yield paths to other targets that could be valid") {
    val graph = `(a)<--(b)-->(c)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .withNfa(singleRelPath)
      .into(graph.c)
      .paths()

    paths shouldBe Seq(Seq(graph.b, graph.bc, graph.c))
  }

  test("non-inlined prefilter applies before K limit") {
    val graph = `(n1)-->(n2)-->(n3)`

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(anyDirectedPath)
      .filter(p => p.length > 1)
      .withK(1)
      .paths()

    paths shouldBe Seq(Seq(graph.n1, graph.r1_2, graph.n2, graph.r2_3, graph.n3))
  }

  test("non-inlined prefilter applies after projection") {
    val graph = `(n1)-->(n2)-->(n3)`

    val lengths = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(anyDirectedPath)
      .project(TracedPath.fromSignpostStack(_).length)
      .filter(_ > 0)
      .toList

    lengths shouldBe Seq(1, 2)
  }

  test("bidirectional search does not yield a repeated relationship if encountered from the RHS") {
    /*
      (n1)--(n2)--(n3)--(n4)--(n5)--()
      /  \
     ()   ()
     */
    val graph = TestGraph.builder
    val n1 = graph.node()
    val n2 = graph.node()
    val n3 = graph.node()
    val n4 = graph.node()
    val n5 = graph.node()

    val r6 = graph.rel(n1, n2)
    val r7 = graph.rel(n2, n3)
    val r8 = graph.rel(n3, n4)
    val r9 = graph.rel(n4, n5)

    graph.rel(n5, graph.node())

    graph.rel(n1, graph.node())
    graph.rel(n1, graph.node())

    val paths = fixture()
      .withGraph(graph.build())
      .from(n1)
      .into(n5)
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val b = sb.newState("b")
        val c = sb.newState("c")
        val t = sb.newState("t", isFinalState = true)

        s.addNodeJuxtaposition(a)
        a.addRelationshipExpansion(b, direction = Direction.BOTH)
        b.addRelationshipExpansion(c, direction = Direction.BOTH)
        c.addNodeJuxtaposition(a)
        c.addNodeJuxtaposition(t)
      }
      .logged()
      .paths()

    paths shouldBe Seq(Seq(n1, n1, r6, n2, r7, n3, n3, r8, n4, r9, n5, n5))
  }

  test("max depth") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(b, c)
    val ac = graph.rel(a, c)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(c)
      .withMaxDepth(1)
      .withNfa(anyDirectedPath)
      .paths()

    paths shouldBe Seq(Seq(a, ac, c))
  }

  /*******************************************
   * Algorithm introspection via event hooks *
   *******************************************/

  test("intoTarget initiates a bidirectional search") {
    val graph = TestGraph.builder
    val s1 = graph.node()
    val n2 = graph.node()
    val n3 = graph.node()
    val t4 = graph.node()

    graph.rel(s1, n2)
    graph.rel(s1, n3)
    graph.rel(n2, t4)

    val events = fixture()
      .withGraph(graph.build())
      .from(s1)
      .into(t4)
      .withK(1)
      .withNfa(anyDirectedPath)
      .events()

    events should (contain(Expand(TraversalDirection.BACKWARD, 2, 1)))
  }

  test("bidirectional search stops searching if the component around the target is exhausted") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(a, c)

    val d = graph.node()

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(d)
      .withK(1)
      .withNfa(`(s) ((a)-->(b))* (t)`)
      .events()

    events.ofType[ExpandNode] shouldBe Seq(
      ExpandNode(a, TraversalDirection.FORWARD),
      ExpandNode(d, TraversalDirection.BACKWARD)
    )
    events.ofType[ReturnPath] shouldBe Seq.empty
  }

  test("bidirectional search stops searching if the component around the source is exhausted") {
    val graph = TestGraph.builder
    val a = graph.node()

    val b = graph.node()

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(b)
      .withK(1)
      .withNfa(`(s) ((a)-->(b))* (t)`)
      .events()

    events.ofType[ExpandNode] shouldBe Seq(ExpandNode(a, TraversalDirection.FORWARD))
    events.ofType[ReturnPath] shouldBe Seq.empty
  }

  test("intoTarget stops searching the graph after the target is saturated") {
    val graph = `(n1)-->(n2)-->(n3)`

    val events = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .into(graph.n2)
      .withK(1)
      .withNfa(anyDirectedPath)
      .events()

    events should (contain(NextLevel(1)) and not contain NextLevel(2))
  }

  test("propagation is performed when a previously-seen node is encountered") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    val d = graph.node()
    val ab = graph.rel(a, b)
    val bc = graph.rel(b, c)
    val ad = graph.rel(a, d)
    val db = graph.rel(d, b)

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(c, SearchMode.Unidirectional)
      .withNfa(anyDirectedPath)
      .events()

    val expected = new EventRecorder()
      .nextLevel(2)
      .schedulePropagation(b, 2, 1)
      .returnPath(a, ab, b, bc, c)
      .nextLevel(3)
      .propagateLengthPair(b, 2, 1)
      .returnPath(a, ad, d, db, b, bc, c)
      .nextLevel(4)
      .getEvents

    events should contain inOrderElementsOf expected
  }

  test("node should not be considered a target if it does not match the intoTarget specifier") {
    val graph = `(a)<--(b)-->(c)`

    val events = fixture()
      .withGraph(graph.graph)
      .from(graph.b)
      .into(graph.c)
      .withNfa(anyDirectedPath)
      .events()

    events should (contain(AddTarget(graph.c)) and not contain AddTarget(graph.a))
  }

  test("algorithm should exit once we hit max depth") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val c = graph.node()
    graph.rel(a, b)
    graph.rel(b, c)
    graph.rel(a, c)

    val events = fixture()
      .withGraph(graph.build())
      .from(a)
      .into(c)
      .withMaxDepth(1)
      .withNfa(anyDirectedPath)
      .events()

    events should not contain NextLevel(2)
  }

  test("multiple relationship expansion does not prevent backward bidirectional search") {
    val graph = `(n1)-->(n2)<--(n3)-->(n4)<--(n5)-->(n6)<--(n7)`

    val events = fixture()
      .withGraph(graph.graph)
      .from(graph.n3)
      .into(graph.n7)
      .withNfa(`(s) ((a)-->(b)<--(c))+ (t)`)
      .events()

    events should contain(ExpandNode(graph.n5, TraversalDirection.BACKWARD))
  }

  // ignored because we still have some rogue cursor interactions somewhere
  ignore("multiple relationship expansion does not cause more cursor interactions than single rel equivalent") {
    val graph = `(n1)-->(n2)<--(n3)-->(n4)<--(n5)-->(n6)<--(n7)`

    val f = fixture()
      .withGraph(graph.graph)
      .from(graph.n3)

    def getCursorEvents(nfa: PGStateBuilder => Unit) =
      f.withNfa(nfa)
        .events()
        .collect {
          case e: CursorSetNode          => e
          case e: CursorNextRelationship => e
        }
        .groupBy(identity)
        .view
        .mapValues(_.length)
        .toMap

    val mre = getCursorEvents(`(s) ((a)--(b)--(c))* (t)`)
    val sre = getCursorEvents(`(s) ((a)--(b)--(c))* (t) [single transition]`)

    mre shouldBe sre
  }

  test("predicate is not evaluated twice on the same interior MRE node") {
    val graph = TestGraph.fromTemplate("""
                 .-----.
                 v     |
          (s)-->( )-->( )
           ^     |
           '-----'
        """)

    val count = mutable.Map.empty[Long, Int]
    def predicate(node: Long): Boolean = {
      count.updateWith(node) {
        case None    => Some(1)
        case Some(x) => Some(x + 1)
      }
      true
    }

    val lengths = fixture()
      .withGraph(graph.graph)
      .from(graph node "s")
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val c = sb.newState("c")
        val t = sb.newState("t", isFinalState = true)

        s.addNodeJuxtaposition(a)
        a.addMultiRelationshipExpansion(
          c,
          r(direction = Direction.OUTGOING).n("b", predicate).r(direction = Direction.OUTGOING)
        )
        c.addNodeJuxtaposition(a)
        c.addNodeJuxtaposition(t)
      }
      .toList
      .map(_.length)

    lengths shouldBe Seq(2, 2, 4)

    count should not be empty
    Inspectors.forAll(count.values) { _ shouldBe 1 }
  }

  test("predicate is not evaluated twice on the same interior MRE relationship") {

    // the idea here is we will traverse r twice, in two iterations of the MRE, and the second time we should
    // not rerun the predicate
    val graph = TestGraph.fromTemplate("""
      .---.
      '->(1)-[r]->(2)<-.
                   '---'
        """)

    val count = mutable.Map.empty[Long, Int]
    def predicate(rel: RelationshipTraversalEntities): Boolean = {
      count.updateWith(rel.relationshipReference()) {
        case None    => Some(1)
        case Some(x) => Some(x + 1)
      }
      true
    }

    val lengths = fixture()
      .withGraph(graph.graph)
      .from(graph node "1")
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val c = sb.newState("c")
        val t = sb.newState("t", isFinalState = true)

        s.addNodeJuxtaposition(a)
        a.addMultiRelationshipExpansion(
          c,
          r(direction = Direction.BOTH, predicate = predicate).n("b").r(direction = Direction.OUTGOING)
        )
        c.addNodeJuxtaposition(a)
        c.addNodeJuxtaposition(t)
      }
      .toList
      .map(_.length)

    lengths shouldBe Seq(2, 2)
    count should not be empty
    Inspectors.forAll(count.values) { _ shouldBe 1 }
  }

  test(
    "compound predicate in multiple relationship expansion is evaluated with correct nodes in backward bidirectional search"
  ) {
    // the double outgoing rel at node 2 makes the algorithm prefer the backwards expansion of node 4
    val graph = TestGraph.fromTemplate("""
     (1)-->(2)-->(3)-->(4)
            |
            v
           ( )
      """)

    // in the predicate we rely on the fact that the test graph nodes are generated with ascending ID from left to right
    (1 to 4).foreach { id =>
      graph node id.toString shouldBe id
    }

    val paths = fixture()
      .withGraph(graph.graph)
      .from(graph node "2")
      .into(graph node "4")
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val b = sb.newState("b")
        val t = sb.newState("t", isFinalState = true)

        val predicate: CompoundPredicate =
          (startNode: Long, _: Array[VirtualRelationshipValue], _: Array[Long], endNode: Long) =>
            startNode < endNode

        s.addNodeJuxtaposition(a)
        a.addMultiRelationshipExpansion(
          b,
          Array(new MultiRelationshipExpansion.Rel(_ => true, null, Direction.OUTGOING, SlotOrName.None)),
          Array.empty,
          predicate
        )
        b.addNodeJuxtaposition(a)
        b.addNodeJuxtaposition(t)
      }
      .toList

    paths should not be empty
  }

  /****************************************
   * Target node pruning regression cases *
   ****************************************/

  test("target node purging challenger") {

    // This test highlights a problem that any target signpost purging solution we've tried has failed to solve.
    // Namely that after we've purged target a given signpost, which we later re-register towards a new target,
    // we're not guaranteed to register it at its minimum distance first.

    // We first explain the ingredients to the test, after which we explain the cause of failure
    //
    // INGREDIENTS:
    //
    // * A SHORTEST 2 path selector
    //
    // * We need a target signpost `problem_sp` which lies on two paths to a target t1, p1 of length l1 and p2 of length l2
    //   where, problem_sp is at index i1 in p1 and i2 in p2, where:
    //   - l1 < l2
    //   - l2 - i2 < l1 - i1    (I.e even if p1 is shorter, the subpath that remains after tsp is shorter in p2.)
    //   - the path p0 = p1.subpath(0, i1).concat(p2.subpath(i2, l2)) shouldn't be a trail in the data graph
    //
    // * We then need a target t2 further away, that has t1 on it's way back from source like
    //    (s) -- ... -->(t) --- ... --->(t2)
    //
    // * And we need a longer path from (s) to tsp.prevNode which is completely disconnected from the rest
    //   of the graph, and longer than the shortest path we trace from (s) to (t2)
    //
    // Model for how the p0,p1,p2 paths can look:
    //
    //  p0 = (s)-...->()-[dup]->()-...->()-[problem_sp]->()-...->()-[dup]->()-...->(t)
    //
    //  p1 = (s)-...->()-[dup]->()-...->()-[problem_sp]->()-...->(t)
    //
    //  p2 = (s)-...->()-[tsp]->()-...->()-[problem_sp]->()-...->(t)
    //
    //
    // This model fits the following pattern,
    //
    //  (s) (()-[:DUP|R1|R3]->())+ ()-[:PROBLEM]->() (()-[:DUP|R2]->())+ (()-[:REST]->())* (:T)
    //
    // on the following graphs,
    //
    //
    //                    ┌───────────PROBLEM────────────┐
    //                    │            ┌──┐              │
    //                    │    ┌──R1───▶n2│───R1───┐     │
    //                    │ ┌──┴─┐     └──┘       ┌▼───┐ │
    //                    │ │    │                │    ├─┘
    //                    └─▶n0:S│──────DUP──────▶│ n1 │      ┌─────┐      ┌───┐      ┌───┐      ┌─────┐
    //                      │    │                │    ├─REST─▶n13:T├─REST─▶n14├─REST─▶n15├─REST─▶n16:T│
    //                      └┬─┬─┘ ┌──┐    ┌──┐   └▲─▲─┘      └─────┘      └───┘      └───┘      └─────┘
    //                       │ └R2─▶n3├─R2─▶n4├─R2─┘ │
    //    ┌─────────R3───────┘     └──┘    └──┘      └────────R3───────┐
    //    │┌──┐    ┌──┐    ┌──┐    ┌──┐    ┌──┐    ┌───┐    ┌───┐    ┌─┴─┐
    //    └▶n5├─R3─▶n6├─R3─▶n7├─R3─▶n8├─R3─▶n9├─R3─▶n10├─R3─▶n11├─R3─▶n12│
    //     └──┘    └──┘    └──┘    └──┘    └──┘    └───┘    └───┘    └───┘

    // where the paths p0,p1,p2 are given by
    //
    // p0 = (s)-[:DUP]->(n1)-[sp_problem:PROBLEM]->(s)-[:DUP]->()-[:REST]->(t1)
    //
    // p1 = (s)-[:DUP]->(n1)-[sp_problem:PROBLEM]->(s)-[:R1 * 2]->()-[:REST]->(t1)
    //
    // p2 = (s)-[:R2 *3]->(n1)-[sp_problem:PROBLEM]->(s)-[:DUP]->()-[:REST]->(t1)
    //
    //
    // CAUSE OF FAILURE:
    //
    // This is what will happen (in an algorithm with faulty target signpost purging):
    //
    // * We will trace p0 first, but since it isn't a trail in the data graph, we'll prune (among other source signposts)
    //   the source signpost corresponding to sp_problem at lengthFromSource == 3. We will however add
    //   minDistToTarget==2 to the target signpost corresponding to sp_problem
    //
    // * We then trace p1 and add lengthFromSource == 4 to the source signpost corresponding to sp_problem.
    //   We also decrement the remainingTargetCount to 1
    //
    // * We then trace p2 and add lengthFromSource == 5 to the source signpost corresponding to sp_problem.
    //   We also decrement the remainingTargetCount to 0, and set minDistToTarget==NO_SUCH_DISTANCE in the
    //   target signpost corresponding to sp_problem during target signpost purging
    //
    // * Then when the BFS reaches t2, it will try to trace the path from (s) to (t2) of length 7, however,
    //   when the tracer reaches (t1) it will stop since (t1) won't have any source signpost with
    //   lengthFromSource==4, since this was pruned when we traced p0. This is still correct!
    //   When it tries to trace this path, it will register target signposts the whole way from t1 to t2,
    //   and t1 will be registered to propagate the length data corresponding to p1,
    //
    // * We will then trace the path
    //
    //    (s)-[:DUP]->(n1)-[sp_problem:PROBLEM]->(s)-[:R1 * 2]->()-[:REST]->(t1)-[:REST * 3]->(t2),
    //
    //   and when we do this we will re-set the minDistToTarget in the target signpost corresponding to sp_problem
    //   to minDistToTarget=7. THIS IS INCORRECT! Indeed, sp_problem can follow an even shorter path to t2, namely,
    //
    //    (n1)-[sp_problem:PROBLEM]->(s)-[:DUP]->()-[:REST]->(t1)-[:REST * 3]->(t2)
    //
    //   with a distance to target of 6.
    //
    // * This makes it so that the longer subpath through all the :R3 rels are propagated up a non-shortest
    //   path to a target it's first propagation, and won't be traced with the shorter path to the target which we
    //   missed.

    val graph = TestGraph.fromTemplate("""
                       .----------[:PROBLEM]----------.
                       | .----[:R1]--->()---[:R1]---. |
                       v |                          v |
                     (START)---------[:DUP]-------->( )-[:REST]->(:T)-[:REST]->()-[:REST]->()-[:REST]->(:T)
                       | |                          ^ ^
      .-----[:R3]------' '[:R2]->()-[:R2]->()-[:R2]-' '------[:R3]---------.
      v                                                                    |
     ()-[:R3]->()-[:R3]->()-[:R3]->()-[:R3]->()-[:R3]->()-[:R3]->()-[:R3]->()
        """)

    val R1 = graph relTypes "R1"
    val R2 = graph relTypes "R2"
    val R3 = graph relTypes "R3"
    val DUP = graph relTypes "DUP"
    val PROBLEM = graph relTypes "PROBLEM"
    val REST = graph relTypes "REST"

    val nfa = {
      import NfaDsl.Implicits._
      import scala.language.postfixOps
      // format: off
      "s"
        .|> ("a"-(DUP:|R1:|R3)->"b"+)
        .|> ("c"-PROBLEM->"d")
        .|> ("e"-(DUP:|R2)->"f"+)
        .|> ("g"-REST->"h"*)
        .|> ("t" where graph.hasLabel("T"))
      // format: on
    }

    val lengths = fixture()
      .withGraph(graph.graph)
      .withNfa(nfa.build)
      .from(graph node "START")
      .withK(2)
      .project(TracedPath.fromSignpostStack(_).length)
      .filter(len => len != 9 && len != 10)
      .toList

    lengths shouldBe Seq(5, 6, 8, 15)
  }

  /**
  Walk mode
   */
  test("support walk into") {
    val graph = `(n1)-->(n2)`
    val nfa = `(s) ((a)--(b))+ (t)`

    val paths: Seq[Seq[Long]] = fixture()
      .withGraph(graph.graph)
      .withNfa(nfa)
      .withK(5)
      .from(graph.n1)
      .into(graph.n2)
      .withMatchMode(TraversalMatchMode.Walk)
      .withMode(SearchMode.Unidirectional)
      .paths()

    paths shouldBe Seq(
      // (n1)-->(n2)
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2),
      // (n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      )
    )
  }

  test("support walk") {
    val graph = `(n1)-->(n2)`
    val nfa = `(s) ((a)--(b))+ (t)`

    val paths: Seq[Seq[Long]] = fixture()
      .withGraph(graph.graph)
      .withNfa(nfa)
      .withK(5)
      .from(graph.n1)
      .withMatchMode(TraversalMatchMode.Walk)
      .withMode(SearchMode.Unidirectional)
      .paths()

    paths shouldBe Seq(
      // (n1)-->(n2)
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2),
      // (n1)-->(n2)-->(n1)
      Seq(graph.n1, graph.n1, graph.r1_2, graph.n2, graph.n2, graph.r1_2, graph.n1, graph.n1),
      // (n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1
      ),

      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2
      ),
      // (n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)-->(n2)-->(n1)
      Seq(
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1,
        graph.r1_2,
        graph.n2,
        graph.n2,
        graph.r1_2,
        graph.n1,
        graph.n1
      )
    )
  }

  /*******************
   * Memory tracking *
   *******************/

  test("memory tracking") {
    val graph = `(n1)-->(n2)`

    val mt = new LocalMemoryTracker()

    // TODO tests with NO_TRACKING
    // ignore the memory tracker for path tracer for the purposes of this test
    def createPathTracer(_mt: MemoryTracker, hooks: PPBFSHooks): PathTracer[TracedPath] =
      new PathTracer(
        EmptyMemoryTracker.INSTANCE,
        TraversalMatchModeFactory.trailMode(EmptyMemoryTracker.INSTANCE, hooks),
        hooks
      )

    val iter = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(anyDirectedPath)
      .withMemoryTracker(mt)
      .build(createPathTracer)

    val heap1 = mt.estimatedHeapMemory()

    iter.next() // a

    val heap2 = mt.estimatedHeapMemory()
    heap1 should be < heap2

    iter.next() // b

    val heap3 = mt.estimatedHeapMemory()
    heap2 should be < heap3

    iter.close()

    val heap4 = mt.estimatedHeapMemory()
    heap4 shouldBe 0
  }

  /****************
   * Interruption *
   ****************/

  test("can be interrupted by AssertOpen check") {
    val graph = `(n1)-->(n2)`

    val iter = fixture()
      .withGraph(graph.graph)
      .from(graph.n1)
      .withNfa(anyDirectedPath)
      .onAssertOpen {
        throw new Exception("boom")
      }
      .build()

    the[Exception] thrownBy iter.asScala.toList should have message "boom"
  }

  /*********************************************************
   * Examples of NFAs which currently break the algorithm! *
   *********************************************************/

  ignore("single node with loop in graph, single state with loop in NFA") {
    val graph = TestGraph.builder
    val a = graph.node()
    val r1 = graph.rel(a, a)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", true, true)
        s.addRelationshipExpansion(s, direction = Direction.OUTGOING)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a),
      Seq(a, r1, a)
    )
  }

  ignore("an r* pattern should be able to be represented by a single NFA state with loop RE") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", true, true)
        s.addRelationshipExpansion(s)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a),
      Seq(a, ab, b)
    )
  }

  ignore("an r* pattern should be able to be represented by a two NFA states with loop RE then NJ") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        s.addRelationshipExpansion(s)
        val e = sb.newState("e", isFinalState = true)
        s.addNodeJuxtaposition(e)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a),
      Seq(a, ab, b)
    )
  }

  ignore("an r+ pattern should be able to be represented by two NFA states with an RE and NJ") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val e = sb.newState("e", isFinalState = true)

        s.addRelationshipExpansion(e)
        e.addNodeJuxtaposition(s)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, ab, b)
    )
  }

  ignore("an r+ pattern should be able to be represented by three NFA states with a final NJ") {
    val graph = TestGraph.builder
    val a = graph.node()
    val b = graph.node()
    val ab = graph.rel(a, b)

    val paths = fixture()
      .withGraph(graph.build())
      .from(a)
      .withNfa { sb =>
        val s = sb.newState("s", isStartState = true)
        val a = sb.newState("a")
        val e = sb.newState("e", isFinalState = true)

        s.addRelationshipExpansion(a)
        a.addNodeJuxtaposition(s)
        a.addNodeJuxtaposition(e)
      }
      .from(a)
      .paths()

    paths shouldBe Seq(
      Seq(a, ab, b)
    )
  }
}

object PGPathPropagatingBFSTest {

  private def anyDirectedPath(sb: PGStateBuilder): Unit = {
    val s = sb.newState(isStartState = true, isFinalState = true)
    s.addRelationshipExpansion(s, direction = Direction.OUTGOING)
  }

  private def singleRelPath(sb: PGStateBuilder): Unit = {
    sb.newState(isStartState = true).addRelationshipExpansion(sb.newState(isFinalState = true))
  }

  // noinspection TypeAnnotation
  private object `(n1)-->(n2)` {
    private val g = TestGraph.builder
    val n1 = g.node()
    val n2 = g.node()
    val r1_2 = g.rel(n1, n2)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)<--(b)-->(c)` {
    private val g = TestGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val ba = g.rel(b, a)
    val bc = g.rel(b, c)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(n1)-->(n2)-->(n3)` {
    private val g = TestGraph.builder
    val n1 = g.node()
    val n2 = g.node()
    val n3 = g.node()
    val r1_2 = g.rel(n1, n2)
    val r2_3 = g.rel(n2, n3)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)-->(b)<--(c)` {
    private val g = TestGraph.builder
    val a = g.node()
    val b = g.node()
    val c = g.node()
    val ab = g.rel(a, b)
    val cb = g.rel(c, b)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(n1)-->(n2)<--(n3)-->(n4)<--(n5)-->(n6)<--(n7)` {
    private val g = TestGraph.builder
    private val n1 = g.node()
    private val n2 = g.node()
    val n3 = g.node()
    val n4 = g.node()
    val n5 = g.node()
    val n6 = g.node()
    val n7 = g.node()

    g.rel(n1, n2)
    g.rel(n3, n2)
    val r3_4 = g.rel(n3, n4)
    val r5_4 = g.rel(n5, n4)
    val r5_6 = g.rel(n5, n6)
    val r7_6 = g.rel(n7, n6)
    val graph = g.build()
  }

  // noinspection TypeAnnotation
  private object `(a)<--(b)-->(a)` {
    private val g = TestGraph.builder
    val a = g.node()
    val b = g.node()
    val ba1 = g.rel(b, a)
    val ba2 = g.rel(b, a)
    val graph = g.build()
  }

}
