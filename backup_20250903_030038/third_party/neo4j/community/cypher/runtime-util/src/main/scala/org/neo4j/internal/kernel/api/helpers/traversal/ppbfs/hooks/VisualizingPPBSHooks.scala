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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks

import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost

import scala.collection.mutable

/**
 * Prints a graphviz visualization of the final state of the product graph to stdout.
 *
 * Useful for debugging *small* graph+nfa combinations!
 */
class VisualizingPPBSHooks extends PPBFSHooks {
  val nodes = mutable.HashSet.empty[NodeState]
  val rels = mutable.HashSet.empty[TwoWaySignpost]

  override def discover(node: NodeState, direction: TraversalDirection): Unit = {
    nodes.add(node)
  }

  override def addSourceSignpost(signpost: TwoWaySignpost, lengthFromSource: Int): Unit = {
    rels.add(signpost)
  }

  override def addTargetSignpost(signpost: TwoWaySignpost, lengthToTarget: Int): Unit = {
    rels.add(signpost)
  }

  override def finished(): Unit = {
    def nodeName(n: NodeState) = {
      val state = n.state().slotOrName() match {
        case SlotOrName.VarName(name, _) => name
        case _                           => n.state().id.toString
      }
      s""""(${n.id},$state)""""
    }

    val vertices = nodes
      .toSeq
      .sortBy(_.id())
      .map(nodeName)
      .mkString("\n")

    val edges = rels.map { rel =>
      val sb = new StringBuilder()
        .append(nodeName(rel.prevNode))
        .append(" -> ")
        .append(nodeName(rel.forwardNode))
        .append("[")

      if (!rel.lengths.isEmpty) {
        sb
          .append("headlabel=<<font color=\"darkgreen\">")
          .append(rel.lengths.renderSourceLengths())
          .append("</font>>")
      }

      if (rel.minTargetDistance != -1) {
        sb
          .append(" taillabel=<<font color=\"purple\">")
          .append(rel.minTargetDistance)
          .append("</font>>")
      }

      sb
        .append("]")
        .append(";")
        .toString()
    }.mkString("\n")

    val columns = {
      val sb = new StringBuilder()
      sb.append("edge[weight=1000 style=invis]\n")
      nodes.groupBy(_.state.id)
        .foreach { case (_, ns) =>
          sb.append(ns.toSeq.sortBy(_.id).map(nodeName).mkString(" -> ")).append("\n")
        }
      sb.toString()
    }

    val ranks = nodes.groupBy(_.id())
      .map { case (_, ns) =>
        val sb = new StringBuilder()
        sb.append("rank=same { edge[weight=1000 style=invis] ")
        sb.append(ns.toSeq.sortBy(_.state.id).map(nodeName).mkString(" -> "))
        sb.append("}")
        sb.toString()
      }.mkString("\n")

    val graph = s"""
                   |digraph {
                   |// styling
                   |edge[labelfontsize=7 arrowsize=0.3 penwidth=0.5 labelfontname=courier]
                   |node[fontsize=8 fontname=courier]
                   |
                   |// node states
                   |$vertices
                   |
                   |// signposts
                   |$edges
                   |
                   |// these invisible heavily-weighted edges force the layout engine to form a grid:
                   |
                   |$columns
                   |$ranks
                   |}""".stripMargin

    println(graph)
  }
}
