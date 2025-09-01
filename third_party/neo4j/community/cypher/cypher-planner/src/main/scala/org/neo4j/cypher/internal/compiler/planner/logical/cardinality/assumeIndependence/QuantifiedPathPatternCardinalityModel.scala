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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults.DEFAULT_REL_UNIQUENESS_SELECTIVITY
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.topDown

import scala.collection.mutable
import scala.util.chaining.scalaUtilChainingOps

trait QuantifiedPathPatternCardinalityModel extends NodeCardinalityModel with PatternRelationshipCardinalityModel {

  def getQuantifiedPathPatternCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    quantifiedPathPattern: QuantifiedPathPattern,
    uniqueRelationships: Set[LogicalVariable],
    boundaryNodePredicates: Set[Predicate]
  ): Cardinality = {
    val predicates = QuantifiedPathPatternPredicates.partitionSelections(labelInfo, quantifiedPathPattern.selections)

    lazy val labelsOnFirstNode =
      predicates.labelsOnNodes(
        quantifiedPathPattern.leftBinding.outer,
        quantifiedPathPattern.leftBinding.inner
      )
    lazy val labelsOnLastNode =
      predicates.labelsOnNodes(
        quantifiedPathPattern.rightBinding.outer,
        quantifiedPathPattern.rightBinding.inner
      )

    lazy val (predicatesSolvedForFirstIteration, otherPredicates) =
      partitionBoundarySolvedPredicates(quantifiedPathPattern, predicates, boundaryNodePredicates)

    lazy val extraRelTypeInfo = quantifiedPathPattern.patternRelationships.collect {
      case PatternRelationship(rel, _, _, Seq(relType), _) => rel -> relType
    }.toMap

    lazy val boundaryNodePredicatesSelectivity: Selectivity =
      context.predicatesSelectivityWithExtraRelTypeInfo(
        labelInfo = predicates.allLabelInfo,
        extraRelTypeInfo = extraRelTypeInfo,
        predicates = predicatesSolvedForFirstIteration
      )

    lazy val otherPredicatesSelectivity: Selectivity =
      context.predicatesSelectivityWithExtraRelTypeInfo(
        labelInfo = predicates.allLabelInfo,
        extraRelTypeInfo = extraRelTypeInfo,
        predicates = otherPredicates
      )

    object inferLabels {

      private lazy val nodeConnections: Seq[PatternRelationship] =
        quantifiedPathPattern.patternRelationships.iterator.toSeq

      def apply(context: QueryGraphCardinalityContext)(labelInfo: LabelInfo)
        : (LabelInfo, QueryGraphCardinalityContext) = {
        context.labelInferenceStrategy.inferLabels(context, labelInfo, nodeConnections)
      }

      lazy val forJunctionNode: (Set[LabelName], QueryGraphCardinalityContext) = {

        val leftInner = quantifiedPathPattern.leftBinding.inner
        val rightInner = quantifiedPathPattern.rightBinding.inner

        // Junction node is both the `rightInner` node of the current iteration, and the `leftInner` node of the next iteration.
        // We add node connections from the next iteration (renaming variable to match the current iteration) to the list
        // to potentially enable better label inference.
        val nodeConnectionsWithJunction: Seq[PatternRelationship] = {
          val junctionNodeConnections = nodeConnections.collect {
            case r if r.left == leftInner  => r.withLeft(rightInner)
            case r if r.right == leftInner => r.withRight(rightInner)
          }
          nodeConnections ++ junctionNodeConnections
        }

        val labelInfoWithJunction = predicates.allLabelInfo
          .updated(rightInner, predicates.labelsOnNodes(leftInner, rightInner))

        context.labelInferenceStrategy.inferLabels(context, labelInfoWithJunction, nodeConnectionsWithJunction) pipe {
          case (labelInfo, context) => (labelInfo.getOrElse(rightInner, Set.empty), context)
        }
      }
    }

    def iterationCardinality(
      context: QueryGraphCardinalityContext,
      leftBindingInnerLabels: Set[LabelName],
      rightBindingInnerLabels: Set[LabelName]
    ): Cardinality = {
      inferLabels(context) {
        predicates.allLabelInfo
          .updated(quantifiedPathPattern.leftBinding.inner, leftBindingInnerLabels)
          .updated(quantifiedPathPattern.rightBinding.inner, rightBindingInnerLabels)
      } pipe {
        case (iterationLabels, context) =>
          getPatternRelationshipsCardinality(context, iterationLabels, quantifiedPathPattern.patternRelationships)
      }
    }

    def iterationMultiplier(
      context: QueryGraphCardinalityContext,
      leftBindingInnerLabels: Set[LabelName],
      rightBindingInnerLabels: Set[LabelName],
      junctionNodeCardinality: Cardinality
    ): Multiplier = {

      val cardinality = iterationCardinality(
        context,
        leftBindingInnerLabels,
        rightBindingInnerLabels
      )
      Multiplier.ofDivision(cardinality, junctionNodeCardinality)
        .getOrElse(Multiplier.ZERO)
    }

    val patternCardinality =
      RepetitionCardinalityModel
        .quantifiedPathPatternRepetitionAsRange(quantifiedPathPattern.repetition)
        .view
        .map {
          case 0 =>
            getEmptyPathPatternCardinality(
              context,
              predicates.allLabelInfo,
              quantifiedPathPattern.left,
              quantifiedPathPattern.right
            )

          case 1 =>
            val singleIterationLabels =
              predicates.allLabelInfo
                .updated(quantifiedPathPattern.leftBinding.inner, labelsOnFirstNode)
                .updated(quantifiedPathPattern.rightBinding.inner, labelsOnLastNode)
            val uniquenessSelectivity = DEFAULT_REL_UNIQUENESS_SELECTIVITY ^ predicates.differentRelationships.size
            val patternCardinality =
              getPatternRelationshipsCardinality(
                context,
                singleIterationLabels,
                quantifiedPathPattern.patternRelationships
              )
            patternCardinality * uniquenessSelectivity * otherPredicatesSelectivity

          case i =>
            inferLabels.forJunctionNode pipe {
              case (labelsOnJunctionNode, context) =>
                val junctionNodeCardinality =
                  resolveNodeLabels(context, labelsOnJunctionNode)
                    .map(getLabelsCardinality(context, _))
                    .getOrElse(Cardinality.EMPTY)

                val firstIterationCardinality = iterationCardinality(
                  context,
                  leftBindingInnerLabels = labelsOnFirstNode,
                  rightBindingInnerLabels = labelsOnJunctionNode
                )

                val intermediateIterationMultiplier = iterationMultiplier(
                  context,
                  leftBindingInnerLabels = labelsOnJunctionNode,
                  rightBindingInnerLabels = labelsOnJunctionNode,
                  junctionNodeCardinality = junctionNodeCardinality
                )

                val lastIterationMultiplier = iterationMultiplier(
                  context,
                  leftBindingInnerLabels = labelsOnJunctionNode,
                  rightBindingInnerLabels = labelsOnLastNode,
                  junctionNodeCardinality = junctionNodeCardinality
                )

                val uniqueRelationshipsInPattern =
                  uniqueRelationships.intersect(quantifiedPathPattern.relationshipVariableGroupings.map(_.group))
                val uniquenessSelectivity =
                  RepetitionCardinalityModel.relationshipUniquenessSelectivity(
                    differentRelationships = predicates.differentRelationships.size,
                    uniqueRelationships = uniqueRelationshipsInPattern.size,
                    repetitions = i
                  )

                firstIterationCardinality *
                  (intermediateIterationMultiplier ^ (i - 2)) *
                  lastIterationMultiplier *
                  uniquenessSelectivity *
                  (otherPredicatesSelectivity ^ i) *
                  (boundaryNodePredicatesSelectivity ^ (i - 1))
            }
        }.sum(NumericCardinality)

    patternCardinality
  }

  /**
   * Partition qpp predicates depending on whether they've already been solved once on the boundary node or not.
   * It's important to treat these predicates separately in order to not overestimate their selectivity.
   * Since they already exist on the boundary node, we exclude their selectivity for the first iteration of the qpp,
   * since it's already solved for that iteration.
   *
   * @param qpp The quantified path pattern
   * @param predicates The predicates associated with the quantified path pattern
   * @param boundaryNodePredicates all predicates that are present on the boundary nodes
   *                               (juxtaposed nodes outside of the quantified path pattern)
   * @return A tuple containing predicates solved once on boundary nodes, and all other predicates
   */
  private def partitionBoundarySolvedPredicates(
    qpp: QuantifiedPathPattern,
    predicates: QuantifiedPathPatternPredicates,
    boundaryNodePredicates: Set[Predicate]
  ): (Set[Predicate], Set[Predicate]) = {
    val rewriter = (binding: NodeBinding) =>
      topDown(Rewriter.lift {
        case variable if variable == binding.outer => binding.inner
      })

    val leftBindingRewriter = rewriter(qpp.leftBinding)
    val rightBindingRewriter = rewriter(qpp.rightBinding)

    // rewrite boundary node predicates to their corresponding inner representation
    val rewrittenPredicates = boundaryNodePredicates.flatMap(pred =>
      Seq(leftBindingRewriter(pred), rightBindingRewriter(pred))
    )

    predicates.otherPredicates.partition(rewrittenPredicates.contains)
  }

  private def getPatternRelationshipsCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    patternRelationships: NonEmptyList[PatternRelationship]
  ): Cardinality = {
    val firstRelationship = patternRelationships.head
    val firstRelationshipCardinality = getSimpleRelationshipCardinality(
      context = context,
      labelInfo = labelInfo,
      leftNode = firstRelationship.left,
      rightNode = firstRelationship.right,
      relationshipTypes = firstRelationship.types,
      relationshipDirection = firstRelationship.dir
    )
    val otherRelationshipsMultiplier = patternRelationships.tail.view.map { relationship =>
      Multiplier.ofDivision(
        dividend = getSimpleRelationshipCardinality(
          context = context,
          labelInfo = labelInfo,
          leftNode = relationship.left,
          rightNode = relationship.right,
          relationshipTypes = relationship.types,
          relationshipDirection = relationship.dir
        ),
        divisor = getNodeCardinality(context, labelInfo, relationship.left).getOrElse(Cardinality.EMPTY)
      ).getOrElse(Multiplier.ZERO)
    }.product(NumericMultiplier)
    firstRelationshipCardinality * otherRelationshipsMultiplier
  }
}

case class QuantifiedPathPatternPredicates(
  allLabelInfo: LabelInfo,
  differentRelationships: Set[DifferentRelationships],
  otherPredicates: Set[Predicate]
) {
  def labelsOnNode(nodeName: LogicalVariable): Set[LabelName] = allLabelInfo.getOrElse(nodeName, Set.empty)

  def labelsOnNodes(nodeNames: LogicalVariable*): Set[LabelName] = nodeNames.foldLeft(Set.empty[LabelName]) {
    case (labels, nodeName) => labels.union(labelsOnNode(nodeName))
  }
}

object QuantifiedPathPatternPredicates {

  def partitionSelections(labelInfo: LabelInfo, selections: Selections): QuantifiedPathPatternPredicates = {
    val labelsBuilder = mutable.Map.empty[LogicalVariable, mutable.Builder[LabelName, Set[LabelName]]]
    val differentRelationshipsBuilder = Set.newBuilder[DifferentRelationships]
    val otherPredicatesBuilder = Set.newBuilder[Predicate]
    selections.predicates.foreach {
      case Predicate(_, HasLabels(v: Variable, labels)) =>
        labelsBuilder.updateWith(v)(builder => Some(builder.getOrElse(Set.newBuilder).addOne(labels.head)))
      case Predicate(_, differentRelationships: DifferentRelationships) =>
        differentRelationshipsBuilder.addOne(differentRelationships)
      case otherPredicate =>
        otherPredicatesBuilder.addOne(otherPredicate)
    }
    labelInfo.foreach {
      case (name, labels) =>
        labelsBuilder.updateWith(name)(builder => Some(builder.getOrElse(Set.newBuilder).addAll(labels)))
    }
    QuantifiedPathPatternPredicates(
      allLabelInfo = labelsBuilder.view.mapValues(_.result()).toMap,
      differentRelationships = differentRelationshipsBuilder.result(),
      otherPredicates = otherPredicatesBuilder.result()
    )
  }
}
