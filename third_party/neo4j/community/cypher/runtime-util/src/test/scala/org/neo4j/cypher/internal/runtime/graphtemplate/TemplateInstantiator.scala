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
package org.neo4j.cypher.internal.runtime.graphtemplate

import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import org.neo4j.values.virtual.PathReference
import org.neo4j.values.virtual.VirtualValues

trait TemplateInstantiator[NODE, REL] {
  def createNode(labels: Seq[String]): NODE
  def createRel(from: NODE, to: NODE, relType: Option[String]): REL
}

case class InstantiatedGraph[NODE, REL](
  namedNodes: Map[String, NODE],
  nodes: Seq[NODE],
  namedRels: Map[String, REL],
  rels: Seq[REL]
) {
  def node(name: String): NODE = namedNodes(name)
  def rel(name: String): REL = namedRels(name)
}

object InstantiatedGraph {

  implicit class DbInstantiatedGraph(graph: InstantiatedGraph[Node, Relationship]) {

    def pathReference(names: String*): PathReference = {
      val nodes = names.zipWithIndex.collect { case (n, i) if i % 2 == 0 => graph.node(n).getId }
      val rels = names.zipWithIndex.collect { case (r, i) if i % 2 == 1 => graph.rel(r).getId }

      VirtualValues.pathReference(
        nodes.toArray,
        rels.toArray
      )
    }
  }
}

class TransactionTemplateInstantiator(tx: Transaction, defaultRelType: String = "R")
    extends TemplateInstantiator[Node, Relationship] {

  def createNode(labels: Seq[String]): Node =
    tx.createNode(labels.map(Label.label): _*)

  def createRel(from: Node, to: Node, relType: Option[String]): Relationship =
    from.createRelationshipTo(to, RelationshipType.withName(relType.getOrElse(defaultRelType)))
}
