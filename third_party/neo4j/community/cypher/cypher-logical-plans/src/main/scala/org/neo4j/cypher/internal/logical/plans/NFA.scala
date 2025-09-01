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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.ExhaustiveNodeConnection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.NodeConnections
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA.MultiRelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.RelationshipExpansionTransition
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFA.Transition
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Repetition

import scala.collection.immutable.ArraySeq

object NFA {

  case class PathLength(min: Int, maybeMax: Option[Int]) {
    def addMin(addition: Int): PathLength = PathLength(min + addition, maybeMax)

    def addMax(maybeAddition: Option[Int]): PathLength =
      PathLength(min, maybeAddition.flatMap(addition => maybeMax.map(_ + addition)))
  }

  object PathLength {
    val none: PathLength = PathLength(0, None)

    def from(nodeConnections: NodeConnections[ExhaustiveNodeConnection]): PathLength =
      nodeConnections.connections.foldLeft(PathLength(0, Some(0)): PathLength) {
        case (pathLength, nodeConnection) => nodeConnection match {
            case PatternRelationship(_, _, _, _, length) =>
              length match {
                case SimplePatternLength        => pathLength.addMin(1).addMax(Some(1))
                case VarPatternLength(min, max) => pathLength.addMin(min).addMax(max)
              }
            case QuantifiedPathPattern(_, _, _, _, Repetition(qppMin, qppMax), _, relationshipVariableGroupings) =>
              val amountOfRelationships = relationshipVariableGroupings.size
              val max = qppMax.limit.map(_.toInt * amountOfRelationships)
              val min = qppMin.toInt * amountOfRelationships
              pathLength.addMin(min).addMax(max)
          }

      }
  }

  object State {
    private[plans] def expressionStringifier: ExpressionStringifier = ExpressionStringifier(_.asCanonicalStringVal)
  }

  /**
   * A State is associated with a node (variable) and an ID that must be unique per NFA.
   *
   * It may have a predicate on the node which must be satisfied to enter the state.
   * E.g., if this predicate is part of a transition from state a to state b, and the pattern looks like this:
   * {{{(a) ( (b:B)-->() )*}}}
   * then the variablePredicate should contain `b:B`
   */
  case class State(id: Int, variable: LogicalVariable, variablePredicate: Option[VariablePredicate]) {

    def toDotString: String = {
      val nodeWhere = variablePredicate.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      s"\"($id, ${variable.name}$nodeWhere)\""
    }
  }

  object Transition {

    /**
     * Extract the endId
     */
    def unapply(t: Transition): Some[Int] = Some(t.endId)
  }

  /**
   * A (potentially conditional) transition between two states.
   * The start state is omitted, since it is the map key in the [[NFA.transitions]] map.
   */
  sealed trait Transition {

    /**
     * The end state of this transition.
     */
    val endId: Int

    /**
     * The variable of the predicate, for conditional transitions.
     * @return
     */
    def predicateVariables: Seq[LogicalVariable]
    def variablePredicates: Seq[VariablePredicate]

    /** All variables named in the transition regardless of whether they are part of a predicate */
    def variables: Seq[LogicalVariable]
    def toDotString: String
  }

  /**
   * A node juxtaposition transition. This transition is unconditional.
   */
  case class NodeJuxtapositionTransition(endId: Int) extends Transition {
    override def predicateVariables: Seq[LogicalVariable] = Seq.empty
    override def variablePredicates: Seq[VariablePredicate] = Seq.empty
    override def variables: Seq[LogicalVariable] = Seq.empty
    override def toDotString: String = ""
  }

  /**
   * A relationship expansion transition. This transition can be conditional.
   *
   * @param predicate the condition under which the transition may be applied.
   * @param endId     the end state of this transition.
   */
  case class RelationshipExpansionTransition(predicate: RelationshipExpansionPredicate, endId: Int) extends Transition {
    override def predicateVariables: Seq[LogicalVariable] = predicate.relPred.map(_.variable).toSeq
    override def variablePredicates: Seq[VariablePredicate] = predicate.relPred.toSeq
    override def variables: Seq[LogicalVariable] = Seq(predicate.relationshipVariable)
    override def toDotString: String = predicate.toDotString
  }

  /**
   * A multi-relationship expansion transition. This transition can be conditional.
   *
   * In this qpp, we have a repeated section with multiple relationships. That repeated section is what
   * [[MultiRelationshipExpansionTransition]] refers to:
   *
   * (s) ((a:A)-[r1:R1]-(b:B)-[r2:R2]-(c:C) WHERE a.prop = c.prop)+ (t)
   *
   * Specifically, the transition contains the relationships and the *interior* nodes. In this case, we have
   * - relPredicates: [ r1:R1, r2:R2 ]
   * - nodePredicates: [ b:B ] (nb: not a or c)
   * - compoundPredicate: a.prop = c.prop
   *
   * The predicate `a.prop = c.prop` applies across the whole transition and is evaluated once it has been expanded,
   * and it can reference any node or relationship (including boundary nodes `a` and `c`) within the transition, the
   * source node (`s`) and in the case of a bidirectional search it can also reference the target node (`t`).
   */
  case class MultiRelationshipExpansionTransition(
    relPredicates: Seq[RelationshipExpansionPredicate],
    nodePredicates: Seq[NodeExpansionPredicate],
    compoundPredicate: Option[Expression],
    endId: Int
  ) extends Transition {

    override def predicateVariables: Seq[LogicalVariable] =
      relPredicates.flatMap(_.variable) ++ nodePredicates.map(_.nodeVariable)

    override def variablePredicates: Seq[VariablePredicate] =
      relPredicates.flatMap(_.relPred) ++ nodePredicates.flatMap(_.nodePred)

    override def variables: Seq[LogicalVariable] =
      relPredicates.map(_.relationshipVariable) ++ nodePredicates.map(_.nodeVariable)

    override def toDotString: String = {
      ("" +: nodePredicates.map(p => p.toDotString)).zip(relPredicates.map(p => p.toDotString)).map {
        case (node, rel) => node + rel
      }.mkString("") + compoundPredicate.map(p => s" WHERE ${State.expressionStringifier(p)}").getOrElse("")
    }
  }

  /**
   * This predicate is used for a relationship in a pattern.
   * There is an optional variablePredicate (`relPred`) for the relationship of this transition.
   *
   * Below, we use this pattern as an example, assuming we're transitioning from a to b.
   * {{{(a)-[r:R|Q WHERE r.prop = 5]->(b)}}}
   *
   * @param relationshipVariable the relationship. `r` in the example.
   * @param relPred the predicate for the relationship `r.prop = 5` in the example.
   * @param types the allowed types of the relationship. This is not inlined into relPred because this predicate can be
   *              executed more efficiently. `Seq(R,Q)` in the example.
   * @param dir the direction of the relationship, from the perspective of the start of the transition.
   *            `OUTGOING` in the example.
   */
  case class RelationshipExpansionPredicate(
    relationshipVariable: LogicalVariable,
    relPred: Option[VariablePredicate],
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ) {

    def variable: Option[LogicalVariable] =
      relPred.map(_.variable)

    def toDotString: String = {
      val (dirStrA, dirStrB) = LogicalPlanToPlanBuilderString.arrows(dir)
      val typeStr = LogicalPlanToPlanBuilderString.relTypeStr(types)
      val relWhere = relPred.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      s"$dirStrA[${relationshipVariable.name}$typeStr$relWhere]$dirStrB"
    }

    /**
     * A predicate expressing the types that this relationship must have.
     */
    def relationshipTypePredicate: Option[Expression] = {
      Option.when(types.nonEmpty)(HasTypes(relationshipVariable, types)(InputPosition.NONE))
    }

  }

  case class NodeExpansionPredicate(
    nodeVariable: LogicalVariable,
    nodePred: Option[VariablePredicate]
  ) {

    def toDotString: String = {
      val nodeWhere = nodePred.map(vp => s" WHERE ${State.expressionStringifier(vp.predicate)}").getOrElse("")
      s"(${nodeVariable.name}$nodeWhere)"
    }
  }
}

/**
 * A non-deterministic finite automaton, used to solve SHORTEST.
 *
 * @param states the states of the automaton
 * @param transitions a map that maps each state id to its outgoing transitions.
 * @param startId the start state id. Must be an element of states.
 * @param finalId the final state id. Must be an element of states.
 */
case class NFA(
  states: ArraySeq[State],
  transitions: Map[Int, Set[Transition]],
  startId: Int,
  finalId: Int
) {

  def variables: Set[LogicalVariable] = {
    val stateNames = states.iterator.map(_.variable)
    val transitionNames = transitions.values.flatten.iterator.flatMap(_.variables)
    (stateNames ++ transitionNames).toSet
  }

  def startState: State = states(startId)
  def finalState: State = states(finalId)

  /**
   * All predicates in the NFA.
   * Note that this contains relationship type predicates (which are not expresses as [[VariablePredicate]]s).
   */
  def predicates: Set[Expression] =
    (states.iterator.flatMap(_.variablePredicate).map(_.predicate) ++
      transitions.values.flatten.iterator.flatMap { t =>
        val relTypePredicate = t match {
          case _: NFA.NodeJuxtapositionTransition            => Seq.empty
          case RelationshipExpansionTransition(predicate, _) => predicate.relationshipTypePredicate.toSeq
          case MultiRelationshipExpansionTransition(relPredicates, _, compoundPredicate, _) =>
            relPredicates.flatMap(_.relationshipTypePredicate) ++ compoundPredicate
        }
        val variablePredicate = t.variablePredicates.map(_.predicate)
        variablePredicate ++ relTypePredicate
      }).toSet

  /**
   * All the variables used in [[VariablePredicate]]s.
   * Note that this does not contain relationship type predicates.
   */
  def predicateVariables: Set[LogicalVariable] = {
    val statePredicates = states.iterator.flatMap(_.variablePredicate).map(_.variable)
    val inlineTransitionPredicates = transitions.values.flatten.iterator.flatMap(_.predicateVariables)
    val allVariables = this.variables
    val compoundPredicates = transitions.values.flatten.iterator.collect {
      case MultiRelationshipExpansionTransition(_, _, Some(compoundPredicate), _) =>
        compoundPredicate.dependencies intersect allVariables
    }.flatten

    val res = (statePredicates ++ inlineTransitionPredicates ++ compoundPredicates).toSet
    res
  }

  def nodes: Set[LogicalVariable] =
    states.map(_.variable).toSet ++ states.flatMap(_.variablePredicate).map(_.variable)

  def relationships: Set[LogicalVariable] =
    transitions.iterator
      .flatMap(_._2)
      .collect { case t: RelationshipExpansionTransition => t.predicate }
      .flatMap(r => Iterator(Some(r.relationshipVariable), r.relPred.map(_.variable)).flatten)
      .toSet

  override def toString: String = {
    val states = this.states.toList.sortBy(_.id)
    val transitions = this.transitions.view.mapValues(
      _.toList.sortBy(_.endId).mkString("[\n  ", "\n  ", "\n]")
    ).toList.sortBy(_._1).mkString("\n")
    s"NFA($states,\n$transitions\n, $startState, $finalState)"
  }

  /**
   * @return a DOT String to generate a graphviz. For example use
   *         https://dreampuf.github.io/GraphvizOnline/
   */
  def toDotString: String = {
    val nodes = states
      .sortBy(_.id)
      .map { node =>
        val style = if (node.id == startState.id) " style=filled" else ""
        val shape = if (node.id == finalState.id) " shape=doublecircle" else ""
        s"""  ${node.id} [label=${node.toDotString}$style$shape];"""
      }.mkString("\n")
    val edges =
      transitions.toSeq
        .flatMap { case (start, transitions) => transitions.map(start -> _) }
        .sortBy {
          case (start, Transition(endId)) => (start, endId)
        }
        .map {
          case (start, t @ Transition(endId)) =>
            s"""  $start -> $endId [label="${t.toDotString}"];"""
        }
        .mkString("\n")
    s"digraph G {\n$nodes\n$edges\n}"
  }

  AssertMacros.checkOnlyWhenAssertionsAreEnabled(
    states.zipWithIndex.forall { case (s, i) => s.id == i },
    "NFA States should be sorted by internal State Id"
  )
}
