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
package org.neo4j.cypher.internal.ir.helpers.overlaps

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.ir.CreatesDynamicNodeLabels
import org.neo4j.cypher.internal.ir.CreatesDynamicRelationshipType
import org.neo4j.cypher.internal.ir.CreatesKnownPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNoPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesNodeLabels
import org.neo4j.cypher.internal.ir.CreatesPropertyKeys
import org.neo4j.cypher.internal.ir.CreatesRelationshipType
import org.neo4j.cypher.internal.ir.CreatesStaticNodeLabels
import org.neo4j.cypher.internal.ir.CreatesStaticRelationshipType
import org.neo4j.cypher.internal.ir.CreatesUnknownPropertyKeys
import org.neo4j.cypher.internal.label_expressions.SolvableLabelExpression

object CreateOverlaps {

  /**
   * Checks whether the labels and the property keys on the node being created might overlap with the predicates of the node being matched on.
   * @param predicates Predicates of the node being matched on. All predicates must apply exclusively to the node at hand, there is no checks for variable name.
   * @param nodeLabelsToCreate Labels on the node being created.
   * @param propertiesToCreate Keys of the properties being created.
   * @return Whether the existing node and the node to create might overlap.
   */
  def findNodeOverlap(
    predicates: Seq[Expression],
    nodeLabelsToCreate: CreatesNodeLabels,
    propertiesToCreate: CreatesPropertyKeys
  ): Option[NodeOverlap] = {
    val (unsupportedExpressions, nodePredicates) = extractEntityPredicates(predicates)

    findPropertiesOverlap(propertiesToCreate, nodePredicates.properties).flatMap { propertiesOverlap =>
      nodeLabelsToCreate match {
        case CreatesStaticNodeLabels(staticLabels) =>
          val labelNames = staticLabels.map(_.name)
          Option.when(nodePredicates.labelExpressions.forall(_.matchesLabels(labelNames))) {
            NodeOverlap(unsupportedExpressions, propertiesOverlap, NodeLabelsOverlap.Static(staticLabels))
          }

        case CreatesDynamicNodeLabels =>
          Some(NodeOverlap(unsupportedExpressions, propertiesOverlap, NodeLabelsOverlap.Dynamic))
      }
    }
  }

  /**
   * Checks whether the relationship type and the property keys on the relationship being created might overlap with the predicates of the relationship being matched on.
   * @param predicates Predicates of the node relationship matched on. All predicates must apply exclusively to the relationship at hand, there is no checks for variable name.
   * @param relationshipTypeToCreate Type on the relationship being created.
   * @param propertiesToCreate Keys of the properties being created.
   * @return Whether the existing relationship and the relationship to create might overlap.
   */
  def findRelationshipOverlap(
    predicates: Seq[Expression],
    relationshipTypeToCreate: CreatesRelationshipType,
    propertiesToCreate: CreatesPropertyKeys
  ): Option[RelationshipOverlap] = {
    val (unsupportedExpressions, relationshipPredicates) = extractEntityPredicates(predicates)

    findPropertiesOverlap(propertiesToCreate, relationshipPredicates.properties).flatMap { propertiesOverlap =>
      relationshipTypeToCreate match {
        case CreatesStaticRelationshipType(staticRelationshipType) =>
          val relationshipTypeName = staticRelationshipType.name
          Option.when(relationshipPredicates.labelExpressions.forall(_.matchesLabels(Set(relationshipTypeName)))) {
            RelationshipOverlap(
              unsupportedExpressions,
              propertiesOverlap,
              RelationshipTypeOverlap.Static(staticRelationshipType)
            )
          }
        case CreatesDynamicRelationshipType =>
          Some(RelationshipOverlap(unsupportedExpressions, propertiesOverlap, RelationshipTypeOverlap.Dynamic))
      }
    }
  }

  sealed trait EntityOverlap {
    def unprocessedExpressions: Seq[Expression]
    def propertiesOverlap: PropertiesOverlap
    def nodeLabelsOrRelationshipTypeOverlap: NodeLabelsOrRelationshipTypeOverlap
  }

  /**
   * Details of the potential overlap between two nodes on create.
   * @param unprocessedExpressions The expressions that were not processed by the evaluator.
   * @param propertiesOverlap Set of properties responsible for the overlap.
   * @param nodeLabelsOverlap Node labels responsible for the overlap.
   */
  case class NodeOverlap(
    override val unprocessedExpressions: Seq[Expression],
    override val propertiesOverlap: PropertiesOverlap,
    nodeLabelsOverlap: NodeLabelsOverlap
  ) extends EntityOverlap {
    override def nodeLabelsOrRelationshipTypeOverlap: NodeLabelsOrRelationshipTypeOverlap = nodeLabelsOverlap
  }

  /**
   * Details of the potential overlap between two relationships on create.
   * @param unprocessedExpressions The expressions that were not processed by the evaluator.
   * @param propertiesOverlap Set of properties responsible for the overlap.
   * @param relationshipTypeOverlap Relationship type responsible for the overlap.
   */
  case class RelationshipOverlap(
    override val unprocessedExpressions: Seq[Expression],
    override val propertiesOverlap: PropertiesOverlap,
    relationshipTypeOverlap: RelationshipTypeOverlap
  ) extends EntityOverlap {
    override def nodeLabelsOrRelationshipTypeOverlap: NodeLabelsOrRelationshipTypeOverlap = relationshipTypeOverlap
  }

  /**
   * Details of the overlap between a node being created and an existing node.
   */
  sealed trait PropertiesOverlap

  object PropertiesOverlap {

    /**
     * Known set of properties causing the overlap.
     */
    case class Overlap(properties: Set[PropertyKeyName]) extends PropertiesOverlap

    /**
     * Some unknown properties are being created, and so an overlap cannot be ruled out.
     */
    case object UnknownOverlap extends PropertiesOverlap
  }

  sealed trait NodeLabelsOrRelationshipTypeOverlap

  sealed trait NodeLabelsOverlap extends NodeLabelsOrRelationshipTypeOverlap

  object NodeLabelsOverlap {

    /**
     * Known set of static labels causing the overlap.
     */
    case class Static(labelNames: Set[LabelName]) extends NodeLabelsOverlap

    /**
     * Some dynamic labels are being created, and so an overlap cannot be ruled out.
     */
    case object Dynamic extends NodeLabelsOverlap
  }

  sealed trait RelationshipTypeOverlap extends NodeLabelsOrRelationshipTypeOverlap

  object RelationshipTypeOverlap {

    /**
     * Known static relation type causing the overlap.
     */
    case class Static(relationshipTypeName: RelTypeName) extends RelationshipTypeOverlap

    /**
     * A dynamic relationship type is being created, and so an overlap cannot be ruled out.
     */
    case object Dynamic extends RelationshipTypeOverlap
  }

  /**
   * Predicates that are relevant for calculating the overlap between two entities.
   * @param labelExpressions Conjoint list of label expressions, empty is equivalent to (:%|!%) also known as ().
   * @param properties Name of the properties that must exist on the entity.
   */
  private case class EntityPredicates(
    labelExpressions: Seq[SolvableLabelExpression],
    properties: Set[PropertyKeyName]
  ) {

    def combine(other: EntityPredicates): EntityPredicates =
      EntityPredicates(
        labelExpressions = labelExpressions ++ other.labelExpressions,
        properties = properties.union(other.properties)
      )
  }

  private object EntityPredicates {

    def empty: EntityPredicates =
      EntityPredicates(labelExpressions = Vector.empty, properties = Set.empty)

    def fold(nodePredicates: Seq[EntityPredicates]): EntityPredicates =
      nodePredicates.reduceLeftOption(_.combine(_)).getOrElse(empty)

    def withLabelExpression(labelExpression: SolvableLabelExpression): EntityPredicates =
      empty.copy(labelExpressions = Vector(labelExpression))

    def withProperty(propertyKeyName: PropertyKeyName): EntityPredicates =
      empty.copy(properties = Set(propertyKeyName))
  }

  private def extractEntityPredicates(expressions: Seq[Expression]): (Seq[Expression], EntityPredicates) =
    expressions.flatMap(Expressions.splitExpression).partitionMap(expression =>
      Expressions.extractPropertyExpression(expression).map(EntityPredicates.withProperty)
        .orElse(Expressions.extractLabelExpression(expression).map(EntityPredicates.withLabelExpression))
        .toRight(expression)
    ) match {
      case (unprocessedExpressions, values) => (unprocessedExpressions, EntityPredicates.fold(values))
    }

  /**
   * Checks whether there might be an overlap between the properties of the entity being created and the ones referred to in the predicates of an existing entity.
   * @param propertiesToCreate Properties of the entity being created.
   * @param propertyKeyNames Properties that must be present on an existing entity based on its predicates.
   * @return None if there can be no overlap, details of the overlap otherwise.
   */
  private def findPropertiesOverlap(
    propertiesToCreate: CreatesPropertyKeys,
    propertyKeyNames: Set[PropertyKeyName]
  ): Option[PropertiesOverlap] = {
    propertiesToCreate match {
      case CreatesNoPropertyKeys =>
        if (propertyKeyNames.isEmpty)
          Some(PropertiesOverlap.Overlap(Set.empty))
        else
          None // If the existing node has at least one property, and the created node has no properties, then there can be no overlap.
      case CreatesKnownPropertyKeys(keys) =>
        if (propertyKeyNames.subsetOf(keys)) // Note that the empty set is a subset of any other set.
          Some(PropertiesOverlap.Overlap(propertyKeyNames))
        else
          None // If the existing node has at least one property that the node being created does not have, then there can be no overlap.
      case CreatesUnknownPropertyKeys =>
        Some(PropertiesOverlap.UnknownOverlap)
    }
  }
}
