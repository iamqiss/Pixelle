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

abstract class MemoryLeakTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("var-expand should not leak memory") {
    // given
    val (nodes, _) = givenGraph(circleGraph(1000))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .expand(s"(from)-[*1..4]-(to)")
      .input(nodes = Seq("from"))
      .build()
    consume(execute(logicalQuery, runtime, inputValues(Array(nodes.head))))

    // then
    tx.kernelTransaction().memoryTracker().estimatedHeapMemory() should be <= 8448L /*pipelined morsel buffers have a constant memory footprint*/
  }

  test("pruning-var-expand should not leak memory") {
    // given
    val (nodes, _) = givenGraph(circleGraph(1000))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .pruningVarExpand(s"(from)-[*1..4]-(to)")
      .input(nodes = Seq("from"))
      .build()

    consume(execute(logicalQuery, runtime, inputValues(Array(nodes.head))))

    // then
    tx.kernelTransaction().memoryTracker().estimatedHeapMemory() should be <= 384L /*pipelined morsel buffers have a constant memory footprint*/
  }

  test("bfs-pruning-var-expand should not leak memory") {
    // given
    val (nodes, _) = givenGraph(circleGraph(1000))

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("from", "to")
      .bfsPruningVarExpand(s"(from)-[*1..4]-(to)")
      .input(nodes = Seq("from"))
      .build()

    consume(execute(logicalQuery, runtime, inputValues(Array(nodes.head))))

    // then
    tx.kernelTransaction().memoryTracker().estimatedHeapMemory() should be <= 2208L /*pipelined morsel buffers have a constant memory footprint*/
  }
}
