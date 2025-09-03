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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.LabelExpressionConversion.IllegalLabelExpressionException
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.LabelExpressionConversion.NodeLabelsToCreate
import org.neo4j.cypher.internal.expressions.DynamicLabelExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.DynamicLeaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard

import scala.annotation.tailrec

trait LabelExpressionConversion {

  /**
   * Extracts the node labels to create from a label expression in the context of a CREATE or a MERGE clause.
   * It assumes that the expression has been validated and will throw an exception for disjunctions, negations, and
   * wildcards.
   *
   * @param labelExpression the optional label expression coming from the CREATE or MERGE clause
   * @return the set of static labels and the set of dynamic labels to create
   */
  def extractNodeLabelsToCreate(labelExpression: Option[LabelExpression]): NodeLabelsToCreate =
    recursivelyExtractNodeLabelsToCreate(NodeLabelsToCreate.empty, labelExpression.toSeq)

  @tailrec
  private def recursivelyExtractNodeLabelsToCreate(
    labelsToCreate: NodeLabelsToCreate,
    labelExpressions: Seq[LabelExpression]
  ): NodeLabelsToCreate =
    if (labelExpressions.isEmpty) {
      labelsToCreate
    } else {
      labelExpressions.head match {
        case leaf @ Leaf(name, _) => name match {
            case labelName: LabelName =>
              recursivelyExtractNodeLabelsToCreate(labelsToCreate.addStaticLabelName(labelName), labelExpressions.tail)
            case _ => throw new IllegalLabelExpressionException(leaf)
          }
        case dynamicLeaf @ DynamicLeaf(expr, _) => expr match {
            case DynamicLabelExpression(expression, all) if all =>
              recursivelyExtractNodeLabelsToCreate(
                labelsToCreate.addDynamicLabelNames(expression),
                labelExpressions.tail
              )
            case _ => throw new IllegalLabelExpressionException(dynamicLeaf)
          }
        case ColonConjunction(lhs, rhs, _) =>
          recursivelyExtractNodeLabelsToCreate(labelsToCreate, lhs +: rhs +: labelExpressions.tail)
        case Conjunctions(children, _) =>
          recursivelyExtractNodeLabelsToCreate(labelsToCreate, children ++ labelExpressions.tail)
        case colonDisjunction: ColonDisjunction =>
          throw new IllegalLabelExpressionException(colonDisjunction)
        case disjunctions: Disjunctions =>
          throw new IllegalLabelExpressionException(disjunctions)
        case negation: Negation =>
          throw new IllegalLabelExpressionException(negation)
        case wildcard: Wildcard =>
          throw new IllegalLabelExpressionException(wildcard)
      }
    }
}

object LabelExpressionConversion {

  case class NodeLabelsToCreate(
    staticLabelNames: Set[LabelName],
    dynamicLabelNames: Set[Expression]
  ) {

    def union(otherLabelNames: NodeLabelsToCreate): NodeLabelsToCreate =
      NodeLabelsToCreate(
        staticLabelNames = staticLabelNames.union(otherLabelNames.staticLabelNames),
        dynamicLabelNames = dynamicLabelNames.union(otherLabelNames.dynamicLabelNames)
      )

    def addStaticLabelName(labelName: LabelName): NodeLabelsToCreate =
      copy(staticLabelNames = staticLabelNames.incl(labelName))

    def addDynamicLabelNames(expression: Expression): NodeLabelsToCreate =
      copy(dynamicLabelNames = dynamicLabelNames.incl(expression))

  }

  object NodeLabelsToCreate {

    def empty: NodeLabelsToCreate =
      NodeLabelsToCreate(
        staticLabelNames = Set.empty,
        dynamicLabelNames = Set.empty
      )
  }

  class IllegalLabelExpressionException(labelExpression: LabelExpression)
      extends IllegalArgumentException(
        s"This label expression is not allowed here: $labelExpression. This is a bug and should have been caught by Semantic Analysis."
      )
}
