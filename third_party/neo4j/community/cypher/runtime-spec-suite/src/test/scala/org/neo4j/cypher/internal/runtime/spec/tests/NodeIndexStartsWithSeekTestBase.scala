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
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.schema.IndexType

abstract class NodeIndexStartsWithSeekTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should be case sensitive for STARTS WITH with indexes") {
    givenGraph {
      nodeIndex(IndexType.TEXT, "Label", "text")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 2 == 0 => Map("text" -> "CASE")
          case i if i % 2 == 1 => Map("text" -> "case")
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("x.text AS text")
      .nodeIndexOperator("x:Label(text STARTS WITH 'ca')", indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = List.fill(sizeHint / 2)("case")
    runtimeResult should beColumns("text").withRows(singleColumn(expected))
  }

  test("should be case sensitive for STARTS WITH with unique indexes") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Label", "text")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 2 == 0 => Map("text" -> s"CASE $i")
          case i if i % 2 == 1 => Map("text" -> s"case $i")
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("x.text AS text")
      .nodeIndexOperator("x:Label(text STARTS WITH 'ca')", indexType = IndexType.RANGE, unique = true)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (1 to sizeHint by 2).map(i => s"case $i")
    runtimeResult should beColumns("text").withRows(singleColumn(expected))
  }

  test("should handle null input") {
    givenGraph {
      nodeIndex(IndexType.TEXT, "Label", "text")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 2 == 0 => Map("text" -> "CASE")
          case i if i % 2 == 1 => Map("text" -> "case")
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("x.text AS text")
      .nodeIndexOperator("x:Label(text STARTS WITH ???)", paramExpr = Some(nullLiteral), indexType = IndexType.TEXT)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("text").withNoRows()
  }

  test("should handle non-text input") {
    givenGraph {
      nodeIndex(IndexType.TEXT, "Label", "text")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 2 == 0 => Map("text" -> "CASE")
          case i if i % 2 == 1 => Map("text" -> "case")
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("text")
      .projection("x.text AS text")
      .nodeIndexOperator("x:Label(text STARTS WITH 1337)", indexType = IndexType.TEXT)
      .build()

    // then
    execute(logicalQuery, runtime) should beColumns(("text")).withNoRows()
  }

  test("should cache properties") {
    val nodes = givenGraph {
      nodeIndex(IndexType.RANGE, "Label", "text")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("text" -> i.toString)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "text")
      .projection("cache[x.text] AS text")
      .nodeIndexOperator("x:Label(text STARTS WITH '1')", _ => GetValue, indexType = IndexType.RANGE)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      nodes.zipWithIndex.collect { case (n, i) if i.toString.startsWith("1") => Array[Object](n, i.toString) }
    runtimeResult should beColumns("x", "text").withRows(expected)
  }

  test("should cache properties with a unique index") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Label", "text")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("text" -> i.toString)
        },
        "Label"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "text")
      .projection("cache[x.text] AS text")
      .nodeIndexOperator("x:Label(text STARTS WITH '1')", _ => GetValue, indexType = IndexType.RANGE)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected =
      nodes.zipWithIndex.collect { case (n, i) if i.toString.startsWith("1") => Array[Object](n, i.toString) }
    runtimeResult should beColumns("x", "text").withRows(expected)
  }
}
