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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.internal.helpers.collection.Iterators

import java.util

import scala.jdk.CollectionConverters.IteratorHasAsScala

abstract class RemoveLabelsTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("remove single label") {
    givenGraph {
      nodeGraph(sizeHint, "RemoveMe")
    }

    removeLabelsTest(Seq("RemoveMe"), sizeHint)
  }

  test("remove multiple labels") {
    val nodeCount = 2
    val labels = Seq("RemoveMe", "MeToo", "AndMe")
    givenGraph {
      nodeGraph(2, labels.toSeq: _*)
    }

    removeLabelsTest(Seq("RemoveMe", "MeToo", "AndMe"), nodeCount * labels.size)
  }

  test("remove non-existing label") {
    val nodeCount = 10
    givenGraph {
      nodeGraph(nodeCount, "DontMindMe", "MeNeither")
    }

    removeLabelsTest(Seq("NonExisting", "Another"), 0)
  }

  test("remove subset of labels") {
    givenGraph {
      nodeGraph(7, "Cynist")
      nodeGraph(5, "Utilitarian")
      nodeGraph(3, "Nihilist")
      nodeGraph(2, "Cynist", "Nihilist")
    }

    removeLabelsTest(Seq("Cynist"), 7 + 2)
  }

  test("remove nodes on rhs of apply") {
    val nodeCount = 13
    givenGraph {
      nodeGraph(nodeCount, "DeleteMe", "DeleteMeToo")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.removeLabels("n", "DeleteMe", "DeleteMeToo")
      .|.argument("n")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("n")
      .withStatistics(labelsRemoved = 2 * nodeCount)
    val stream = tx.getAllNodes.stream()
    try {
      stream.filter(_.getLabels.iterator().asScala.nonEmpty).count() shouldBe 0
    } finally {
      stream.close()
    }
  }

  test("not return removed labels") {
    val nodeCount = 2
    val nodes = givenGraph {
      nodeGraph(2, "DeleteMe", "KeepMe")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "labels")
      .projection("n as n", "labels(n) as labels")
      .removeLabels("n", "DeleteMe")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    val expected = nodes.map(node => Array[Object](node, util.Arrays.asList("KeepMe")))

    runtimeResult should beColumns("n", "labels")
      .withRows(expected)
      .withStatistics(labelsRemoved = nodeCount)
  }

  test("should not remove too many labels if setLabel is between two loops with continuation") {
    val nodes = givenGraph {
      nodeGraph(sizeHint, "Label")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .removeLabels("n", "Label")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(labelsRemoved = sizeHint)
  }

  private def removeLabelsTest(removeLabels: Seq[String], expectedLabelsRemoved: Int): Unit = {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .removeLabels("n", removeLabels: _*)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult = execute(logicalQuery, runtime)

    runtimeResult should beColumns("n")
      .withStatistics(labelsRemoved = expectedLabelsRemoved)
    removeLabels.foreach { label =>
      withClue(s"Number of nodes with label '$label' ") {
        Iterators.count(tx.findNodes(Label.label(label))) shouldBe 0
      }
    }
  }
}
