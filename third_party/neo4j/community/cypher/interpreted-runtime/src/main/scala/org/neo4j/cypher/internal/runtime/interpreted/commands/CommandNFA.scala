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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.ast.semantics.TokenTable
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.AllocatedTraversalEndpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.CompoundPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.NodePredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.RelationshipQualifiers
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.State
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.function.Predicates
import org.neo4j.internal.kernel.api.RelationshipTraversalEntities
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.MultiRelationshipExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.NodeJuxtaposition
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipPredicate
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

import java.util.function.LongPredicate
import java.util.function.Predicate

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class CommandNFA(
  states: Set[State],
  startState: State,
  finalState: State
) {

  /**
   * Compiles the CommandNFA into [[productgraph.State]]s.
   *
   * @param row row to compile the NFA for
   * @param queryState queryState to compile the NFA for
   * @return the start and end [[productgraph.State]] of the NFA.
   */
  def compile(row: CypherRow, queryState: QueryState): (productgraph.State, productgraph.State) = {

    def bindStatePredicate(predicate: Option[NodePredicate]): LongPredicate =
      predicate.fold(Predicates.ALWAYS_TRUE_LONG) { predicate => l =>
        predicate(row, queryState, VirtualValues.node(l))
      }

    def bindRowPredicate(qualifiers: RelationshipQualifiers): Predicate[RelationshipTraversalEntities] =
      qualifiers.innerRelPred.fold(RelationshipPredicate.ALWAYS_TRUE) { predicate => rel =>
        predicate(row, queryState, rel)
      }

    def bindCompoundPredicate(commandPred: CompoundPredicate): MultiRelationshipExpansion.CompoundPredicate =
      (startNode: Long, rels: Array[VirtualRelationshipValue], interiorNodes: Array[Long], endNode: Long) =>
        commandPred.test(row, queryState, startNode, rels, interiorNodes, endNode)

    val stateLookup = states.map(state =>
      state -> new productgraph.State(
        state.id,
        state.slotOrName,
        bindStatePredicate(state.predicate),
        startState == state,
        finalState == state
      )
    ).toMap

    val reverseNJs = mutable.MultiDict.empty[productgraph.State, NodeJuxtaposition]
    val reverseREs = mutable.MultiDict.empty[productgraph.State, RelationshipExpansion]
    val reverseMREs = mutable.MultiDict.empty[productgraph.State, MultiRelationshipExpansion]

    for ((state, pgState) <- stateLookup) {
      pgState.setNodeJuxtapositions(
        state.nodeTransitions.map { transition =>
          val nj = new NodeJuxtaposition(stateLookup(state), stateLookup(transition.targetState))
          reverseNJs += (nj.targetState() -> nj)
          nj
        }.toArray
      )

      pgState.setRelationshipExpansions(
        state.relTransitions.map { transition =>
          val re = new RelationshipExpansion(
            stateLookup(state),
            bindRowPredicate(transition.relationship),
            if (transition.relationship.types == null) null else transition.relationship.types.types(queryState.query),
            toGraphDb(transition.relationship.dir),
            transition.relationship.slotOrName,
            stateLookup(transition.targetState)
          )
          reverseREs += (re.targetState() -> re)
          re
        }.toArray
      )

      pgState.setMultiRelationshipExpansions(state.multiRelTransitions.map { transition =>
        val mre = new MultiRelationshipExpansion(
          stateLookup(state),
          transition.relationships.map(r =>
            new MultiRelationshipExpansion.Rel(
              bindRowPredicate(r),
              if (r.types == null) null else r.types.types(queryState.query),
              toGraphDb(r.dir),
              r.slotOrName
            )
          ).toArray,
          transition.nodes.map(n =>
            new MultiRelationshipExpansion.Node(bindStatePredicate(n.innerNodePred), n.slotOrName)
          ).toArray,
          bindCompoundPredicate(transition.compoundPredicate),
          stateLookup(transition.targetState)
        )
        reverseMREs += (mre.targetState() -> mre)
        mre
      }.toArray)
    }

    for (pgState <- reverseNJs.keySet) {
      pgState.setReverseNodeJuxtapositions(reverseNJs.get(pgState).toArray)
    }

    for (pgState <- reverseREs.keySet) {
      pgState.setReverseRelationshipExpansions(reverseREs.get(pgState).toArray)
    }

    for (pgState <- reverseMREs.keySet) {
      pgState.setReverseMultiRelationshipExpansions(reverseMREs.get(pgState).toArray)
    }

    (stateLookup(startState), stateLookup(finalState))
  }
}

object CommandNFA {

  private type NodePredicate = (CypherRow, QueryState, AnyValue) => Boolean
  private type RelPredicate = (CypherRow, QueryState, RelationshipTraversalEntities) => Boolean

  class State(
    val id: Int,
    val slotOrName: SlotOrName,
    val predicate: Option[NodePredicate],
    var nodeTransitions: Seq[NodeJuxtapositionTransition],
    var relTransitions: Seq[RelationshipExpansionTransition],
    var multiRelTransitions: Seq[MultiRelationshipExpansionTransition]
  )

  case class NodeJuxtapositionTransition(
    targetState: State
  )

  case class NodeQualifiers(
    innerNodePred: Option[NodePredicate],
    slotOrName: SlotOrName
  )

  case class RelationshipQualifiers(
    innerRelPred: Option[RelPredicate],
    slotOrName: SlotOrName,
    types: RelationshipTypes,
    dir: SemanticDirection
  )

  case class RelationshipExpansionTransition(
    relationship: RelationshipQualifiers,
    targetState: State
  )

  object RelationshipExpansionTransition {

    def apply(
      innerRelPred: Option[RelPredicate],
      slotOrName: SlotOrName,
      types: RelationshipTypes,
      dir: SemanticDirection,
      targetState: State
    ): RelationshipExpansionTransition =
      new RelationshipExpansionTransition(RelationshipQualifiers(innerRelPred, slotOrName, types, dir), targetState)
  }

  case class MultiRelationshipExpansionTransition(
    relationships: Seq[RelationshipQualifiers],
    nodes: Seq[NodeQualifiers],
    compoundPredicate: CompoundPredicate,
    targetState: State
  )

  trait CompoundPredicate {

    def test(
      row: CypherRow,
      state: QueryState,
      startNode: Long,
      rels: Array[VirtualRelationshipValue],
      interiorNodes: Array[Long],
      endNode: Long
    ): Boolean
  }

  object CompoundPredicate {

    val ALWAYS_TRUE: CompoundPredicate =
      (_: CypherRow, _: QueryState, _: Long, _: Array[VirtualRelationshipValue], _: Array[Long], _: Long) => true
  }

  def fromLogicalNFA(
    logicalNFA: NFA,
    predicateToCommand: Expression => commands.predicates.Predicate,
    getSlotOrName: LogicalVariable => SlotOrName = x => SlotOrName.None
  )(implicit st: TokenTable): CommandNFA = {

    def convertNodePredicate(varPredicate: VariablePredicate): NodePredicate = {
      val predicate = predicateToCommand(varPredicate.predicate)
      val offset = ExpressionVariable.cast(varPredicate.variable).offset
      (row: CypherRow, state: QueryState, entity: AnyValue) => {
        state.expressionVariables(offset) = entity
        predicate.isTrue(row, state)
      }
    }

    def convertRelPredicate(varPredicate: VariablePredicate): RelPredicate = {
      val predicate = predicateToCommand(varPredicate.predicate)
      val offset = ExpressionVariable.cast(varPredicate.variable).offset

      val traversalEndpoints = TraversalEndpoint.extract(varPredicate.predicate)

      (row: CypherRow, state: QueryState, rel: RelationshipTraversalEntities) => {
        state.expressionVariables(offset) = ValueUtils.fromRelationshipCursor(rel)

        traversalEndpoints.foreach { case AllocatedTraversalEndpoint(offset, end) =>
          state.expressionVariables(offset) = VirtualValues.node(end match {
            case Endpoint.From => rel.originNodeReference()
            case Endpoint.To   => rel.otherNodeReference()
          })
        }

        predicate.isTrue(row, state)
      }
    }

    def compileStubbedRelationshipExpansion(
      logicalPredicate: NFA.RelationshipExpansionPredicate,
      end: State
    )(implicit st: TokenTable): RelationshipExpansionTransition = {
      val commandRelPred = logicalPredicate.relPred.map(convertRelPredicate)

      // In planner land, empty type seq means all types. We use null in runtime land to represent all types
      val types = logicalPredicate.types
      val relTypes = if (types.isEmpty) null else RelationshipTypes(types.toArray)

      RelationshipExpansionTransition(
        commandRelPred,
        getSlotOrName(logicalPredicate.relationshipVariable),
        relTypes,
        logicalPredicate.dir,
        end
      )
    }

    var startState: State = null
    var finalState: State = null

    // We need to compile the NFA in two phases here due to potential cycles in the NFA

    // first phase: create the states
    val stateLookup = logicalNFA.states.iterator.map { logicalState =>
      val commandState = new State(
        logicalState.id,
        getSlotOrName(logicalState.variable),
        logicalState.variablePredicate.map(convertNodePredicate),
        null,
        null,
        null
      )

      if (logicalNFA.startState == logicalState) {
        assert(startState == null, "There should only be one start state in an NFA")
        startState = commandState
      }
      if (logicalNFA.finalState == logicalState) {
        assert(finalState == null, "There should only be one final state in an NFA")
        finalState = commandState
      }

      logicalState.id -> commandState
    }.toMap

    // second phase: add the transitions
    for (logicalState <- logicalNFA.states) {
      val transitions = logicalNFA.transitions.getOrElse(logicalState.id, Seq.empty)

      val njs = ArrayBuffer.empty[NodeJuxtapositionTransition]
      val res = ArrayBuffer.empty[RelationshipExpansionTransition]
      val mres = ArrayBuffer.empty[MultiRelationshipExpansionTransition]
      transitions.foreach {
        case NFA.NodeJuxtapositionTransition(endId) =>
          val end = logicalNFA.states(endId)
          njs.append(NodeJuxtapositionTransition(stateLookup(end.id)))

        case NFA.RelationshipExpansionTransition(rp: NFA.RelationshipExpansionPredicate, endId) =>
          val end = logicalNFA.states(endId)
          res.append(compileStubbedRelationshipExpansion(rp, stateLookup(end.id)))

        case NFA.MultiRelationshipExpansionTransition(relPredicates, nodePredicates, compoundPredicate, endId) =>
          val end = logicalNFA.states(endId)

          val commandRelPreds = relPredicates.map(rp =>
            RelationshipQualifiers(
              rp.relPred.map(convertRelPredicate),
              getSlotOrName(rp.relationshipVariable),
              // In planner land, empty type seq means all types. We use null in runtime land to represent all types
              if (rp.types.isEmpty) null else RelationshipTypes(rp.types.toArray),
              rp.dir
            )
          )
          val commandNodePreds = nodePredicates.map(np =>
            NodeQualifiers(
              np.nodePred.map(convertNodePredicate),
              getSlotOrName(np.nodeVariable)
            )
          )

          val commandCompoundPred = compoundPredicate.map { expr =>
            val predicate = predicateToCommand(expr)
            val startVar = ExpressionVariable.castOpt(logicalState.variable)
            val endVar = ExpressionVariable.castOpt(end.variable)
            val relVars = relPredicates.map(_.relationshipVariable).map(ExpressionVariable.castOpt)
            val nodeVars = nodePredicates.map(_.nodeVariable).map(ExpressionVariable.castOpt)

            new CompoundPredicate {
              override def test(
                row: CypherRow,
                state: QueryState,
                startNode: Long,
                rels: Array[VirtualRelationshipValue],
                interiorNodes: Array[Long],
                endNode: Long
              ): Boolean = {
                def setVar(optExprVar: Option[ExpressionVariable], value: AnyValue) =
                  optExprVar match {
                    case Some(ev) => state.expressionVariables(ev.offset) = value
                    case None     => ()
                  }

                setVar(startVar, VirtualValues.node(startNode))
                setVar(endVar, VirtualValues.node(endNode))
                var i = 0
                while (i < interiorNodes.length) {
                  setVar(relVars(i), rels(i))
                  setVar(nodeVars(i), VirtualValues.node(interiorNodes(i)))
                  i += 1
                }
                setVar(relVars.last, rels.last)

                predicate.isTrue(row, state)
              }
            }
          }.getOrElse(CompoundPredicate.ALWAYS_TRUE)

          val mre = MultiRelationshipExpansionTransition(
            commandRelPreds,
            commandNodePreds,
            commandCompoundPred,
            stateLookup(end.id)
          )
          mres.append(mre)
      }

      val commandState = stateLookup(logicalState.id)
      commandState.nodeTransitions = njs.toSeq
      commandState.relTransitions = res.toSeq
      commandState.multiRelTransitions = mres.toSeq
    }

    CommandNFA(
      states = stateLookup.values.toSet,
      startState,
      finalState
    )
  }

}
