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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.util.symbols.CypherType

sealed trait CreateConstraintType {
  def description: String
  def predicate: String
}

sealed trait NodeKey extends CreateConstraintType {
  override val description: String = "node key"
}

object NodeKey {
  def cypher25: NodeKey = NodeKeyCypher25
  def cypher5: NodeKey = NodeKeyCypher5
}

private case object NodeKeyCypher25 extends NodeKey {
  override val predicate: String = "IS KEY"
}

private case object NodeKeyCypher5 extends NodeKey {
  override val predicate: String = "IS NODE KEY"
}

sealed trait RelationshipKey extends CreateConstraintType {
  override val description: String = "relationship key"
}

object RelationshipKey {
  def cypher25: RelationshipKey = RelationshipKeyCypher25
  def cypher5: RelationshipKey = RelationshipKeyCypher5
}

private case object RelationshipKeyCypher25 extends RelationshipKey {
  override val predicate: String = "IS KEY"
}

private case object RelationshipKeyCypher5 extends RelationshipKey {
  override val predicate: String = "IS RELATIONSHIP KEY"
}

case object NodePropertyUniqueness extends CreateConstraintType {
  override val description: String = "uniqueness"
  override val predicate: String = "IS UNIQUE"
}

case object RelationshipPropertyUniqueness extends CreateConstraintType {
  override val description: String = "relationship uniqueness"
  override val predicate: String = "IS UNIQUE"
}

case object NodePropertyExistence extends CreateConstraintType {
  override val description: String = "node property existence"
  override val predicate: String = "IS NOT NULL"
}

case object RelationshipPropertyExistence extends CreateConstraintType {
  override val description: String = "relationship property existence"
  override val predicate: String = "IS NOT NULL"
}

case class NodePropertyType(propType: CypherType) extends CreateConstraintType {
  override val description: String = "node property type"
  override val predicate: String = s"IS :: ${propType.description}"
}

case class RelationshipPropertyType(propType: CypherType) extends CreateConstraintType {
  override val description: String = "relationship property type"
  override val predicate: String = s"IS :: ${propType.description}"
}
