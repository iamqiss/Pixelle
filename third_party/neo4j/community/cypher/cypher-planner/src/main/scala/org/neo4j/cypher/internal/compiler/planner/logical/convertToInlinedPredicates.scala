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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.helpers.IterableHelper.RichIterableOnce
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.functions.EndNode
import org.neo4j.cypher.internal.expressions.functions.StartNode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.From
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint.To
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.topDown

object convertToInlinedPredicates {

  sealed trait Mode {
    def predicatesOutsideRepetition: Seq[Expression]
  }

  object Mode {

    case object Trail extends Mode {
      override def predicatesOutsideRepetition: Seq[Expression] = Seq.empty
    }

    /**
    * @param predicatesOutsideRepetition - the predicates on outerStartNode and outerEndNode
    */
    case class Shortest(override val predicatesOutsideRepetition: Seq[Expression]) extends Mode
  }

  /**
   * This method converts the inner predicates of a qpp into node/relationship varlength predicates.
   * This assumes that there is a single relationship pattern within the QPP. This needs to be validated by rewriters themselves since they all have different representations and criteria.
   * It also assumes that the actual predicates have been extracted from the QPP earlier using the extractPredicates/extractQPPPredicates methods.
   * This method will not extractPredicates from within the QPP, since some of the rewriters using this method like the TrailToVarExpandRewriter already receive predicates extracted from the QPP.
   *
   * @param outerStartNode - the outernode of the QPP
   * @param innerStartNode - innernode that starts the relationship pattern
   * @param innerEndNode - the inner node that ends the relationship pattern
   * @param outerEndNode - the outer node after the relationship pattern
   * @param innerRelationship - the relationship variable in the relationship pattern
   * @param predicatesToInline - the predicates on the innerStartNode, innerEndNode and innerRelationship that have to be converted to inlined predicates.
   * @param pathRepetition - the number of times the relationship pattern should repeat
   * @param pathDirection - the direction of the relationship in the pattern
   * @param anonymousVariableNameGenerator - a variable name generator for both node and relationship predicates
   * @param mode - different rules apply for rewriting Trail or Shortest
   * @return maybeInlinedPredicates - inlined node and relationship predicates on new anonymous variables. Returns None if the predicates are not inlinable.
   */
  def apply(
    outerStartNode: LogicalVariable,
    innerStartNode: LogicalVariable,
    innerEndNode: LogicalVariable,
    outerEndNode: LogicalVariable,
    innerRelationship: LogicalVariable,
    predicatesToInline: Seq[Expression],
    pathRepetition: Repetition,
    pathDirection: SemanticDirection,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    mode: Mode
  ): Option[InlinedPredicates] = {
    val anonymousNodeVariable = varFor(anonymousVariableNameGenerator.nextName)
    val anonymousRelationshipVariable = varFor(anonymousVariableNameGenerator.nextName)

    def isAPredicateOnInnerRelationButNotNodes(predicate: Expression): Boolean =
      Set(innerStartNode, innerEndNode, innerRelationship)
        .intersect(predicate.dependencies) == Set(innerRelationship)

    def isAPredicateOnInnerVariables(predicate: Expression): Boolean =
      predicate.dependencies.exists(Set(innerStartNode, innerEndNode, innerRelationship).contains)

    def outsideContainsSamePredicateWithVariable(
      innerPredicate: Expression,
      outerVariable: LogicalVariable
    ): Boolean = {
      mode.predicatesOutsideRepetition.contains(innerPredicate.replaceAllOccurrencesBy(
        innerPredicate.dependencies.head,
        outerVariable
      ))
    }

    def innerPredicatesContainsSamePredicateWithVariable(
      innerPredicate: Expression,
      innerVariable: LogicalVariable
    ): Boolean = {
      predicatesToInline.contains(innerPredicate.replaceAllOccurrencesBy(
        innerPredicate.dependencies.head,
        innerVariable
      ))
    }

    // This is a temporary method, since we need to apply predicates to all nodes in the path.
    // Once we get an API to only apply predicates to the inner-nodes in the path we can get rid of the outer nodes and outer predicates.
    def isJuxtaposedOnOuterNodes(innerPredicate: Expression): Boolean = innerPredicate.dependencies.toSeq match {
      // predicate {prop: 1} is applicable on all nodes if
      // 1. (outerStartNode)((innerStartNode{prop:1})--(innerEndNode))*(outerEndNode{prop:1})
      // 2. (outerStartNode)((innerStartNode{prop:1})--(innerEndNode{prop:1}))+(outerEndNode)
      // 3. (outerStartNode{prop:1})((innerStartNode{prop:1})--(innerEndNode{prop:1}))*(outerEndNode)
      case Seq(`innerStartNode`) =>
        outsideContainsSamePredicateWithVariable(innerPredicate, outerEndNode) ||
        (innerPredicatesContainsSamePredicateWithVariable(innerPredicate, innerEndNode) &&
          (pathRepetition.min > 0 || outsideContainsSamePredicateWithVariable(innerPredicate, outerStartNode)))

      // predicate {prop: 1} is applicable on all nodes if
      // 1. (outerStartNode{prop:1})((innerStartNode)--(innerEndNode{prop:1}))*(outerEndNode)
      // 2. (outerStartNode)((innerStartNode{prop:1})--(innerEndNode{prop:1}))+(outerEndNode)
      // 3. (outerStartNode)((innerStartNode{prop:1})--(innerEndNode{prop:1}))*(outerEndNode{prop:1})
      case Seq(`innerEndNode`) =>
        outsideContainsSamePredicateWithVariable(innerPredicate, outerStartNode) ||
        (innerPredicatesContainsSamePredicateWithVariable(innerPredicate, innerStartNode) &&
          (pathRepetition.min > 0 || outsideContainsSamePredicateWithVariable(innerPredicate, outerEndNode)))
      case _ => false
    }

    def rewriteToRelationshipPredicate(nodePredicate: Expression): Option[Expression] = {
      val rewrittenRelationshipPredicate = pathDirection match {
        case OUTGOING => nodePredicate.endoRewrite(NodeToRelationshipExpressionRewriter(
            startNode = innerStartNode,
            globalRelationshipVariable = innerRelationship,
            endNode = innerEndNode,
            perIterationRelationshipVariable = anonymousRelationshipVariable,
            nameGenerator = anonymousVariableNameGenerator
          ))
        case INCOMING =>
          nodePredicate.endoRewrite(NodeToRelationshipExpressionRewriter(
            startNode = innerEndNode,
            globalRelationshipVariable = innerRelationship,
            endNode = innerStartNode,
            perIterationRelationshipVariable = anonymousRelationshipVariable,
            nameGenerator = anonymousVariableNameGenerator
          ))
        case BOTH =>
          // Replace the innerStartNode by TraversalEndpoint(newAnonymousVariable, From)
          // Replace the innerEndNode by TraversalEndpoint(newAnonymousVariable, To)
          nodePredicate.endoRewrite(NodeToRelationshipExpressionRewriter(
            startNode = innerStartNode,
            globalRelationshipVariable = innerRelationship,
            endNode = innerEndNode,
            perIterationRelationshipVariable = anonymousRelationshipVariable,
            nameGenerator = anonymousVariableNameGenerator,
            isDirected = false
          ))
      }
      Option(rewrittenRelationshipPredicate).filter(pred => {
        pred.dependencies.contains(anonymousRelationshipVariable) || pred.folder.treeExists {
          case _: TraversalEndpoint => true
        }
      })
    }

    val inlinedPredicates =
      predicatesToInline.traverse(predicate => {
        if (isJuxtaposedOnOuterNodes(predicate)) {
          Some(VariablePredicate(
            anonymousNodeVariable,
            predicate
              .replaceAllOccurrencesBy(innerStartNode, anonymousNodeVariable)
              .replaceAllOccurrencesBy(innerEndNode, anonymousNodeVariable)
          ))
        } else if (isAPredicateOnInnerRelationButNotNodes(predicate)) {
          Some(VariablePredicate(
            anonymousRelationshipVariable,
            predicate.replaceAllOccurrencesBy(innerRelationship, anonymousRelationshipVariable)
          ))
        } else if (isAPredicateOnInnerVariables(predicate)) {
          rewriteToRelationshipPredicate(predicate)
            .map(VariablePredicate(anonymousRelationshipVariable, _))
        } else {
          None
        }
      })

    inlinedPredicates.map { inlinedPredicates =>
      val (nodePredicates, relationshipPredicates) =
        inlinedPredicates.toSeq.partition(_.variable == anonymousNodeVariable)
      InlinedPredicates(nodePredicates.distinct, relationshipPredicates)
    }
  }
}

case class InlinedPredicates(
  nodePredicates: Seq[VariablePredicate] = Seq.empty,
  relationshipPredicates: Seq[VariablePredicate] = Seq.empty
)

/**
 * This rewrite will replace all occurrences of the startNode and endNode in the expression.
 * This is used to rewrite expressions in a few QPP based plan rewriters.
 *
 * @param startNode                        - the node to replace with the startNode(rel) (or TraversalEndpoint(rel, From) in case the relationship is undirected)
 * @param globalRelationshipVariable       - the relationship variable that was specified in the query
 * @param endNode                          - the node to replace with the endNode(rel) (or TraversalEndpoint(rel, To) in case the relationship is undirected)
 * @param perIterationRelationshipVariable - the relationship variable to use in the replacement.
 * @param nameGenerator                    - anonymous variable name generator to obtain new distinct variable names to put in the TraversalEndpoint expression
 * @param isDirected                       - whether the relationship is directed or not
 */
case class NodeToRelationshipExpressionRewriter(
  private val startNode: LogicalVariable,
  private val globalRelationshipVariable: LogicalVariable,
  private val endNode: LogicalVariable,
  private val perIterationRelationshipVariable: LogicalVariable,
  private val nameGenerator: AnonymousVariableNameGenerator,
  private val isDirected: Boolean = true
) extends Rewriter {

  private val innerRewriter: Rewriter = Rewriter.lift {
    case `startNode` =>
      if (isDirected)
        StartNode(perIterationRelationshipVariable)(InputPosition.NONE)
      else
        TraversalEndpoint(varFor(nameGenerator.nextName), From)
    case `endNode` =>
      if (isDirected)
        EndNode(perIterationRelationshipVariable)(InputPosition.NONE)
      else
        TraversalEndpoint(varFor(nameGenerator.nextName), To)
    case `globalRelationshipVariable` =>
      perIterationRelationshipVariable
    case AndedPropertyInequalities(v, _, inequalities) if v == startNode || v == endNode =>
      Ands.create(inequalities.map(_.endoRewrite(this)).toListSet)
  }

  private val innerRewriteStopper: RewriterStopper = {
    case _: ExpressionWithComputedDependencies => true
    case _: CachedProperty                     => true
    case _: CachedHasProperty                  => true
    case _                                     => false
  }

  val instance: Rewriter = topDown(rewriter = innerRewriter, stopper = innerRewriteStopper)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
