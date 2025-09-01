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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.LengthBounds
import org.neo4j.cypher.internal.logical.plans.TraversalMatchMode
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.NO_ENTITY_FUNCTION
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaAutoCloseableIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.StatefulShortestPathPipe.traversalMatchModeFactory
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFS
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathWriter
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.SignpostStack
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.values.VirtualValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualValues

import java.util.function.ToLongFunction

case class StatefulShortestPathSlottedPipe(
  source: Pipe,
  sourceSlot: Slot,
  intoTargetSlot: Option[Slot],
  commandNFA: CommandNFA,
  bounds: LengthBounds,
  preFilters: Option[Predicate],
  selector: StatefulShortestPath.Selector,
  groupSlots: List[Int],
  slots: SlotConfiguration,
  reverseGroupVariableProjections: Boolean,
  matchMode: TraversalMatchMode
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  private val getSourceNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(sourceSlot, throwOnTypeError = false)

  private val getTargetNodeFunction: ToLongFunction[ReadableRow] = intoTargetSlot.map(slot =>
    makeGetPrimitiveNodeFromSlotFunctionFor(slot, throwOnTypeError = false)
  ).getOrElse(NO_ENTITY_FUNCTION)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    val nodeCursor = state.query.nodeCursor()
    state.query.resources.trace(nodeCursor)
    val traversalCursor = state.query.traversalCursor()
    state.query.resources.trace(traversalCursor)

    val hooks = PPBFSHooks.getInstance()
    val tracker = traversalMatchModeFactory(matchMode, memoryTracker, hooks)
    val pathTracer =
      new PathTracer[CypherRow](memoryTracker, tracker, hooks)
    val pathPredicate =
      preFilters.fold[java.util.function.Predicate[CypherRow]](_ => true)(pred => pred.isTrue(_, state))

    input.flatMap { inputRow =>
      val sourceNode = getSourceNodeFunction.applyAsLong(inputRow)
      val intoTargetNode = getTargetNodeFunction.applyAsLong(inputRow)
      val (startState, finalState) = commandNFA.compile(inputRow, state)

      PGPathPropagatingBFS.create(
        sourceNode,
        startState,
        intoTargetNode,
        finalState,
        state.query.transactionalContext.dataRead,
        nodeCursor,
        traversalCursor,
        pathTracer,
        withPathVariables(inputRow, _),
        pathPredicate,
        selector.isGroup,
        bounds.max.getOrElse(-1),
        selector.k.toInt,
        commandNFA.states.size,
        memoryTracker,
        hooks,
        state.query.transactionalContext.assertTransactionOpen _,
        tracker
      ).asSelfClosingIterator

    }.closing(nodeCursor).closing(traversalCursor)
  }

  private def withPathVariables(original: CypherRow, stack: SignpostStack): CypherRow = {
    val row = new SlottedRow(slots)
    row.copyAllFrom(original)
    val groupMap = Array.ofDim[ListValueBuilder](slots.numberOfReferences)

    def write(slotOrName: SlotOrName, id: Long, getValue: Long => VirtualValue): Unit =
      slotOrName match {
        case SlotOrName.Slotted(slotOffset, isGroup) =>
          if (isGroup) {
            var builder = groupMap(slotOffset)
            if (builder == null) {
              builder = ListValueBuilder.newListBuilder()
              groupMap(slotOffset) = builder
            }
            builder.add(getValue(id))
          } else {
            row.setLongAt(slotOffset, id)
          }
        case _: SlotOrName.VarName => throw new IllegalStateException("Legacy metadata in Slotted runtime")
        case SlotOrName.None       => ()
      }

    stack.materialize(new PathWriter {
      def writeNode(slotOrName: SlotOrName, id: Long): Unit =
        write(slotOrName, id, VirtualValues.node)

      def writeRel(slotOrName: SlotOrName, id: Long): Unit =
        write(slotOrName, id, VirtualValues.relationship)
    })

    groupSlots.foreach { offset =>
      val value = groupMap(offset) match {
        case null => VirtualValues.EMPTY_LIST
        case list => if (reverseGroupVariableProjections) list.build().reverse() else list.build()
      }
      row.setRefAt(offset, value)
    }

    row
  }
}
