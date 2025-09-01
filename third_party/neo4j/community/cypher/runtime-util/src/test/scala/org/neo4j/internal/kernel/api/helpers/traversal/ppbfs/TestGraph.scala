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

import org.neo4j.cypher.internal.runtime.graphtemplate.InstantiatedGraph
import org.neo4j.cypher.internal.runtime.graphtemplate.TemplateInstantiator
import org.neo4j.cypher.internal.runtime.graphtemplate.parsing.GraphTemplateParser
import org.neo4j.internal.kernel.api.KernelReadTracer
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TestGraph.Rel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TestGraph.TraversedRel
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor.DataGraphRelationshipCursor
import org.neo4j.storageengine.api.RelationshipDirection
import org.neo4j.storageengine.api.RelationshipSelection

import java.util.function.LongPredicate

import scala.collection.mutable

case class TestGraph(nodes: Set[Long], rels: Map[Long, TestGraph.Rel]) {
  def lastId: Long = nodes.size + rels.size
  def nextId: Long = lastId + 1

  def withNode(): TestGraph = copy(nodes = nodes + nextId)

  def withRel(source: Long, target: Long, relType: Int): TestGraph = {
    assert(nodes.contains(source) && nodes.contains(target))
    copy(rels = rels + (nextId -> Rel(nextId, source, target, relType)))
  }

  def rel(id: Long): Option[Rel] = rels.get(id)

  def nodeRels(node: Long): Iterator[(Rel, RelationshipDirection)] =
    rels.values.iterator.collect {
      case rel if node == rel.source && node == rel.target =>
        rel -> RelationshipDirection.LOOP
      case rel if node == rel.source =>
        rel -> RelationshipDirection.OUTGOING
      case rel if node == rel.target =>
        rel -> RelationshipDirection.INCOMING
    }

  def builder = new TestGraph.Builder(this)

  def line(length: Int): TestGraph =
    withBuilder(_.line(length))

  def chainRel(source: Long, target: Long, length: Int): TestGraph =
    withBuilder(_.chainRel(source, target, length))

  private def withBuilder(f: TestGraph.Builder => Unit): TestGraph = {
    val b = builder
    f(b)
    b.build()
  }
}

object TestGraph {

  case class TraversedRel(rel: Rel, from: Long) extends RelationshipTraversalEntities {
    def relationshipReference(): Long = rel.id
    def `type`(): Int = rel.relType
    def sourceNodeReference(): Long = rel.source
    def targetNodeReference(): Long = rel.target

    def otherNodeReference(): Long =
      from match {
        case rel.target => rel.source
        case rel.source => rel.target
        case _          => throw new IllegalArgumentException()
      }
    def originNodeReference(): Long = from
  }

  case class Rel(id: Long, source: Long, target: Long, relType: Int) {
    override def toString: String = s"($source)-[$id:$relType]->($target)"
  }

  val empty: TestGraph = TestGraph(Set.empty, Map.empty)
  def builder = empty.builder

  class Builder(private var graph: TestGraph) {

    def node(): Long = {
      val nextId = graph.nextId
      graph = graph.withNode()
      nextId
    }

    def rel(source: Long, target: Long): Long = rel(source, target, 1)

    def rel(source: Long, target: Long, relType: Int): Long = {
      val nextId = graph.nextId
      graph = graph.withRel(source, target, relType)
      nextId
    }

    def line(length: Int): Seq[Long] = {
      val nodes = (0 to length).map(_ => node())
      nodes.zip(nodes.drop(1)).foreach { case (a, b) => rel(a, b) }
      nodes
    }

    def chainRel(source: Long, target: Long, length: Int): Unit = {
      var current = source
      for (_ <- 1 until length) {
        val next = node()
        rel(current, next)
        current = next
      }
      rel(current, target)
    }

    def build(): TestGraph = graph
  }

  def fromTemplate(template: String): Templated = {
    val graph = TestGraph.builder

    val relTypes = mutable.Map.empty[String, Int]
    val assignedLabels = mutable.MultiDict.empty[Long, String]

    val entities = GraphTemplateParser.parse(template)
      .instantiate(new TemplateInstantiator[Long, Long] {
        def createNode(labels: Seq[String]): Long = {
          val id = graph.node()
          labels.foreach(label => assignedLabels += (id -> label))
          id
        }

        def createRel(from: Long, to: Long, relType: Option[String]): Long = {
          // the default relType is 1, custom relTypes begin at 2
          val relTypeId = relType.map(name => relTypes.getOrElseUpdate(name, relTypes.size + 2)).getOrElse(1)
          graph.rel(from, to, relTypeId)
        }
      })

    val built = graph.build()

    Templated(
      template,
      built,
      entities,
      relTypes.toMap,
      assignedLabels.sets.view.mapValues(_.toSet).toMap
    )
  }

  case class Templated(
    template: String,
    graph: TestGraph,
    entities: InstantiatedGraph[Long, Long],
    relTypes: Map[String, Int],
    labels: Map[Long, Set[String]]
  ) {
    val node = entities.node _
    val rel = entities.rel _

    def hasLabel(label: String): LongPredicate =
      node => labels.get(node).exists(_.contains(label))
  }
}

class MockGraphCursor(graph: TestGraph, hooks: PPBFSHooks) extends DataGraphRelationshipCursor {
  private var rels = Iterator.empty[TraversedRel]
  private var currentNode: Long = -1L
  private var current: TraversedRel = _

  def nextRelationship(): Boolean = {
    hooks.cursorNextRelationship(currentNode)
    if (rels.hasNext) {
      current = rels.next()
      true
    } else {
      current = null
      false
    }
  }

  def setNode(node: Long, selection: RelationshipSelection): Unit = {
    hooks.cursorSetNode(node)
    currentNode = node
    rels = graph.nodeRels(node)
      .collect { case (rel, dir) if selection.test(rel.relType, dir) => TraversedRel(rel, node) }
  }

  def relationshipReference(): Long = this.current.relationshipReference()

  def originNodeReference(): Long = this.currentNode

  def otherNodeReference(): Long = this.current.otherNodeReference()

  def sourceNodeReference(): Long = this.current.sourceNodeReference()

  def targetNodeReference(): Long = this.current.targetNodeReference()

  def `type`(): Int = this.current.`type`()

  def setTracer(tracer: KernelReadTracer): Unit = ()
}
