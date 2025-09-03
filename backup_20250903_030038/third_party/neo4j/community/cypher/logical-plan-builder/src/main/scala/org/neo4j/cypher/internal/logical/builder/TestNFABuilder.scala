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
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder.NodePredicate
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder.unnestRelationshipChain
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA.MultiRelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.NodeExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.NodeJuxtapositionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionPredicate
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFABuilder
import org.neo4j.cypher.internal.rewriting.rewriters.LabelExpressionNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.combineHasLabels
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.inSequence

import scala.annotation.tailrec
import scala.util.control.NonFatal

object TestNFABuilder {

  object NodePredicate {

    def unapply(node: NodePattern): Option[(LogicalVariable, Option[Expand.VariablePredicate])] = node match {
      case NodePattern(Some(nodeVariable: LogicalVariable), labelExpression, None, nodePredicate) =>
        val labelExpressionRewriter = LabelExpressionNormalizer(nodeVariable, Some(NODE_TYPE))
        val normalizedLabelExpression = labelExpression.map(labelExpressionRewriter(_).asInstanceOf[Expression])

        val nodeJointPredicate = (nodePredicate, normalizedLabelExpression) match {
          case (None, None)         => None
          case (Some(p1), None)     => Some(p1)
          case (None, Some(p2))     => Some(p2)
          case (Some(p1), Some(p2)) => Some(Ands(ListSet(p1, p2))(InputPosition.NONE))
        }

        val rewrittenNodeJointPredicate =
          nodeJointPredicate.endoRewrite(inSequence(
            flattenBooleanOperators.instance(CancellationChecker.NeverCancelled),
            combineHasLabels
          ))

        Some((nodeVariable, rewrittenNodeJointPredicate.map(Expand.VariablePredicate(nodeVariable, _))))
      case _ => None
    }
  }

  private def relationshipExpansionPredicate(r: RelationshipPattern): RelationshipExpansionPredicate = r match {
    case RelationshipPattern(
        Some(rel: LogicalVariable),
        relTypeExpression,
        None,
        None,
        relPredicate,
        direction
      ) =>
      val types = LabelExpression.getRelTypes(relTypeExpression)
      val relVariablePredicate = relPredicate.map(Expand.VariablePredicate(rel, _))
      RelationshipExpansionPredicate(
        rel,
        relVariablePredicate,
        types,
        direction
      )
    case _ => throw new IllegalStateException
  }

  @tailrec
  private def unnestRelationshipChain(
    chain: RelationshipChain,
    rels: List[RelationshipExpansionPredicate] = Nil,
    nodes: List[NodeExpansionPredicate] = Nil,
    to: Option[NodeExpansionPredicate] = None
  ): (
    NodeExpansionPredicate,
    Seq[RelationshipExpansionPredicate],
    Seq[NodeExpansionPredicate],
    NodeExpansionPredicate
  ) = chain match {
    case RelationshipChain(
        NodePattern(Some(from: LogicalVariable), None, None, None),
        RelationshipPattern(
          Some(rel: LogicalVariable),
          relTypeExpression,
          None,
          None,
          relPredicate,
          direction
        ),
        NodePredicate(toName, toNodePredicateFromRel)
      ) =>
      val types = LabelExpression.getRelTypes(relTypeExpression)
      val relVariablePredicate = relPredicate.map(Expand.VariablePredicate(rel, _))
      val r = RelationshipExpansionPredicate(
        rel,
        relVariablePredicate,
        types,
        direction
      )
      val toNodePredicate = toNodePredicateFromRel
      val n = NodeExpansionPredicate(toName, toNodePredicate)
      to match {
        case Some(t) =>
          (NodeExpansionPredicate(from, None), r :: rels, n :: nodes, t)
        case None =>
          (NodeExpansionPredicate(from, None), r :: rels, nodes, n)
      }
    case RelationshipChain(
        chain: RelationshipChain,
        rp,
        NodePredicate(toName, toNodePredicateFromRel)
      ) =>
      val r = relationshipExpansionPredicate(rp)
      val toNodePredicate = toNodePredicateFromRel
      val n = NodeExpansionPredicate(toName, toNodePredicate)
      to match {
        case Some(_) =>
          unnestRelationshipChain(chain, r :: rels, n :: nodes, to)
        case None =>
          unnestRelationshipChain(chain, r :: rels, nodes, Some(n))
      }
    case _ => throw new IllegalStateException(s"Illegal relationship chain $chain")
  }
}

/**
 * Builder used to build NFAs in test code together with an [[AbstractLogicalPlanBuilder]].
 */
class TestNFABuilder(startStateId: Int, startStateName: String) extends NFABuilder(startStateId, startStateName) {

  def addTransition(
    fromId: Int,
    toId: Int,
    pattern: String,
    maybeRelPredicate: Option[LogicalVariable => Expression] = None,
    maybeToPredicate: Option[VariablePredicate] = None,
    compoundPredicate: String = ""
  ): TestNFABuilder = {

    parsePattern(pattern) match {

      // (from)-[rel:E]-(to)
      case RelationshipChain(
          NodePattern(Some(from: LogicalVariable), None, None, None),
          RelationshipPattern(
            Some(rel: LogicalVariable),
            relTypeExpression,
            None,
            None,
            relPredicate,
            direction
          ),
          NodePredicate(toName, toNodePredicateFromRel)
        ) =>
        val types = LabelExpression.getRelTypes(relTypeExpression)
        val relVariablePredicate = maybeRelPredicate match {
          case Some(pred) => Some(Expand.VariablePredicate(rel, pred(rel)))
          case None       => relPredicate.map(Expand.VariablePredicate(rel, _))
        }

        val nfaPredicate = RelationshipExpansionPredicate(
          rel,
          relVariablePredicate,
          types,
          direction
        )
        val transition = RelationshipExpansionTransition(nfaPredicate, toId)
        val toNodePredicate = maybeToPredicate match {
          case somePred @ Some(_) => somePred
          case None               => toNodePredicateFromRel
        }
        val fromState = getOrCreateState(fromId, from)
        assertFromNameMatchesFromId(fromState, from.name, fromId, pattern)
        getOrCreateState(toId, toName, toNodePredicate)
        addTransition(fromState, transition)

      // (n1)-[r1:R]->(n2)-[r2:R]->(n3)
      case chain: RelationshipChain =>
        if (maybeRelPredicate.nonEmpty || maybeToPredicate.nonEmpty) {
          throw new IllegalStateException(
            "Multi-Relationship Expansion doesn't support manually constructed predicates"
          )
        }
        val (from, rels, nodes, to) = unnestRelationshipChain(chain)
        val compoundPred = if (compoundPredicate == "") None else Some(Parser.parseExpression(compoundPredicate))
        val fromState = getOrCreateState(fromId, from.nodeVariable)
        assertFromNameMatchesFromId(fromState, from.nodeVariable.name, fromId, pattern)
        val transition = MultiRelationshipExpansionTransition(rels, nodes, compoundPred, toId)
        getOrCreateState(toId, to.nodeVariable, to.nodePred)
        addTransition(fromState, transition)

      case PathConcatenation(Seq(
          NodePattern(Some(from: LogicalVariable), None, None, None),
          NodePredicate(to, toNodePredicateFromPattern)
        )) =>
        val fromState = getOrCreateState(fromId, from)
        assertFromNameMatchesFromId(fromState, from.name, fromId, pattern)
        val transition = NodeJuxtapositionTransition(toId)
        val toNodePredicate = maybeToPredicate match {
          case somePred @ Some(_) => somePred
          case None               => toNodePredicateFromPattern
        }
        getOrCreateState(toId, to, toNodePredicate)
        addTransition(fromState, transition)

      case _ => throw new IllegalArgumentException(s"Expected path pattern or two juxtaposed nodes but was: $pattern")
    }
    this
  }

  def addMultiRelationshipTransition(fromId: Int, toId: Int, pattern: String): TestNFABuilder =
    addMultiRelationshipTransitionWithPredicate(fromId, toId, pattern, "")

  def addMultiRelationshipTransition(
    fromId: Int,
    toId: Int,
    pattern: String,
    compoundPredicate: String
  ): TestNFABuilder =
    addMultiRelationshipTransitionWithPredicate(fromId, toId, pattern, compoundPredicate)

  private def addMultiRelationshipTransitionWithPredicate(
    fromId: Int,
    toId: Int,
    pattern: String,
    compoundPredicate: String
  ): TestNFABuilder = {

    parsePattern(pattern) match {
      // (n1)-[r1:R]->(n2)-[r2:R]->(n3)
      case chain: RelationshipChain =>
        val (from, rels, nodes, to) = unnestRelationshipChain(chain)
        val compoundPred = if (compoundPredicate.trim.isEmpty) None else Some(Parser.parseExpression(compoundPredicate))
        val fromState = getOrCreateState(fromId, from.nodeVariable)
        assertFromNameMatchesFromId(fromState, from.nodeVariable.name, fromId, pattern)
        val transition = MultiRelationshipExpansionTransition(rels, nodes, compoundPred, toId)
        getOrCreateState(toId, to.nodeVariable, to.nodePred)
        addTransition(fromState, transition)

      case _ => throw new IllegalArgumentException(s"Expected path pattern or two juxtaposed nodes but was: $pattern")
    }
    this
  }

  def setFinalState(id: Int): TestNFABuilder = {
    val state = getState(id)
    setFinalState(state)
    this
  }

  private def assertFromNameMatchesFromId(
    actualState: State,
    specifiedName: String,
    fromId: Int,
    pattern: String
  ): Unit = {
    if (actualState.variable.name != specifiedName) {
      throw new IllegalArgumentException(
        s"For id $fromId in pattern '$pattern': expected '${actualState.variable.name}' but was '$specifiedName'"
      )
    }
  }

  private def parsePattern(pattern: String) =
    try {
      Parser.parsePatternElement(pattern)
    } catch {
      case NonFatal(e) =>
        println("Error parsing pattern: " + pattern)
        throw e
    }
}
