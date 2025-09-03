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

sealed trait ShowConstraintType {
  val output: String
  val prettyPrint: String
  val description: String
}

case object AllConstraints extends ShowConstraintType {
  override val output: String = "ALL"
  override val prettyPrint: String = "ALL"
  override val description: String = "allConstraints"
}

sealed trait UniqueConstraints extends ShowConstraintType

object UniqueConstraints {
  def cypher25: UniqueConstraints = UniqueConstraintsCypher25
  def cypher5: UniqueConstraints = UniqueConstraintsCypher5
}

private case object UniqueConstraintsCypher25 extends UniqueConstraints {
  override val output: String = "PROPERTY_UNIQUENESS"
  override val prettyPrint: String = "PROPERTY UNIQUENESS"
  override val description: String = "propertyUniquenessConstraints"
}

private case object UniqueConstraintsCypher5 extends UniqueConstraints {
  override val output: String = "UNIQUENESS"
  override val prettyPrint: String = "UNIQUENESS"
  override val description: String = "uniquenessConstraints"
}

sealed trait NodeUniqueConstraints extends ShowConstraintType

object NodeUniqueConstraints {
  def cypher25: NodeUniqueConstraints = NodeUniqueConstraintsCypher25
  def cypher5: NodeUniqueConstraints = NodeUniqueConstraintsCypher5
}

private case object NodeUniqueConstraintsCypher25 extends NodeUniqueConstraints {
  override val output: String = "NODE_PROPERTY_UNIQUENESS"
  override val prettyPrint: String = "NODE PROPERTY UNIQUENESS"
  override val description: String = "nodePropertyUniquenessConstraints"
}

private case object NodeUniqueConstraintsCypher5 extends NodeUniqueConstraints {
  override val output: String = "UNIQUENESS"
  override val prettyPrint: String = "NODE UNIQUENESS"
  override val description: String = "nodeUniquenessConstraints"
}

sealed trait RelUniqueConstraints extends ShowConstraintType

object RelUniqueConstraints {
  def cypher25: RelUniqueConstraints = RelUniqueConstraintsCypher25
  def cypher5: RelUniqueConstraints = RelUniqueConstraintsCypher5
}

private case object RelUniqueConstraintsCypher25 extends RelUniqueConstraints {
  override val output: String = "RELATIONSHIP_PROPERTY_UNIQUENESS"
  override val prettyPrint: String = "RELATIONSHIP PROPERTY UNIQUENESS"
  override val description: String = "relationshipPropertyUniquenessConstraints"
}

private case object RelUniqueConstraintsCypher5 extends RelUniqueConstraints {
  override val output: String = "RELATIONSHIP_UNIQUENESS"
  override val prettyPrint: String = "RELATIONSHIP UNIQUENESS"
  override val description: String = "relationshipUniquenessConstraints"
}

sealed trait PropExistsConstraints extends ShowConstraintType {
  override val output: String = "PROPERTY_EXISTENCE"
  override val prettyPrint: String = "PROPERTY EXISTENCE"
}

object PropExistsConstraints {
  def cypher25: PropExistsConstraints = PropExistsConstraintsCypher25
  def cypher5: PropExistsConstraints = PropExistsConstraintsCypher5
}

private case object PropExistsConstraintsCypher25 extends PropExistsConstraints {
  override val description: String = "propertyExistenceConstraints"
}

private case object PropExistsConstraintsCypher5 extends PropExistsConstraints {
  override val description: String = "existenceConstraints"
}

sealed trait NodePropExistsConstraints extends ShowConstraintType {
  override val output: String = "NODE_PROPERTY_EXISTENCE"
  override val prettyPrint: String = "NODE PROPERTY EXISTENCE"
}

object NodePropExistsConstraints {
  def cypher25: NodePropExistsConstraints = NodePropExistsConstraintsCypher25
  def cypher5: NodePropExistsConstraints = NodePropExistsConstraintsCypher5
}

private case object NodePropExistsConstraintsCypher25 extends NodePropExistsConstraints {
  override val description: String = "nodePropertyExistenceConstraints"
}

private case object NodePropExistsConstraintsCypher5 extends NodePropExistsConstraints {
  override val description: String = "nodeExistenceConstraints"
}

sealed trait RelPropExistsConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_PROPERTY_EXISTENCE"
  override val prettyPrint: String = "RELATIONSHIP PROPERTY EXISTENCE"
}

object RelPropExistsConstraints {
  def cypher25: RelPropExistsConstraints = RelPropExistsConstraintsCypher25
  def cypher5: RelPropExistsConstraints = RelPropExistsConstraintsCypher5
}

private case object RelPropExistsConstraintsCypher25 extends RelPropExistsConstraints {
  override val description: String = "relationshipPropertyExistenceConstraints"
}

private case object RelPropExistsConstraintsCypher5 extends RelPropExistsConstraints {
  override val description: String = "relationshipExistenceConstraints"
}

case object AllExistsConstraints extends ShowConstraintType {
  override val output: String = "EXISTENCE"
  override val prettyPrint: String = "EXISTENCE"
  override val description: String = "existenceConstraints"
}

case object NodeAllExistsConstraints extends ShowConstraintType {
  override val output: String = "NODE_EXISTENCE"
  override val prettyPrint: String = "NODE EXISTENCE"
  override val description: String = "nodeExistenceConstraints"
}

case object RelAllExistsConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_EXISTENCE"
  override val prettyPrint: String = "RELATIONSHIP EXISTENCE"
  override val description: String = "relationshipExistenceConstraints"
}

case object KeyConstraints extends ShowConstraintType {
  override val output: String = "KEY"
  override val prettyPrint: String = "KEY"
  override val description: String = "keyConstraints"
}

case object NodeKeyConstraints extends ShowConstraintType {
  override val output: String = "NODE_KEY"
  override val prettyPrint: String = "NODE KEY"
  override val description: String = "nodeKeyConstraints"
}

case object RelKeyConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_KEY"
  override val prettyPrint: String = "RELATIONSHIP KEY"
  override val description: String = "relationshipKeyConstraints"
}

case object PropTypeConstraints extends ShowConstraintType {
  override val output: String = "PROPERTY_TYPE"
  override val prettyPrint: String = "PROPERTY TYPE"
  override val description: String = "propertyTypeConstraints"
}

case object NodePropTypeConstraints extends ShowConstraintType {
  override val output: String = "NODE_PROPERTY_TYPE"
  override val prettyPrint: String = "NODE PROPERTY TYPE"
  override val description: String = "nodePropertyTypeConstraints"
}

case object RelPropTypeConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_PROPERTY_TYPE"
  override val prettyPrint: String = "RELATIONSHIP PROPERTY TYPE"
  override val description: String = "relationshipPropertyTypeConstraints"
}

case object RelationshipSourceLabelConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_SOURCE_LABEL"
  override val prettyPrint: String = "RELATIONSHIP SOURCE LABEL"
  override val description: String = "relationshipSourceLabelConstraints"
}

case object RelationshipTargetLabelConstraints extends ShowConstraintType {
  override val output: String = "RELATIONSHIP_TARGET_LABEL"
  override val prettyPrint: String = "RELATIONSHIP TARGET LABEL"
  override val description: String = "relationshipTargetLabelConstraints"
}

case object NodeLabelExistenceConstraints extends ShowConstraintType {
  override val output: String = "NODE_LABEL_EXISTENCE"
  override val prettyPrint: String = "NODE LABEL EXISTENCE"
  override val description: String = "nodeLabelExistenceConstraints"
}
