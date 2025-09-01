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

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingCollections.newArrayDeque
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CastSupport.castOrFail
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.RuntimeMetadataValue
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RepeatPipe.emptyLists
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.expressions.TrailState
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.runtime.slotted.pipes.RepeatSlottedPipe.TrailModeConstraint
import org.neo4j.cypher.internal.runtime.slotted.pipes.RepeatSlottedPipe.TraversalModeConstraint
import org.neo4j.cypher.internal.runtime.slotted.pipes.RepeatSlottedPipe.WalkModeConstraint
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.Measurable
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

import scala.annotation.tailrec

sealed trait SlottedRepeatState {
  val node: Long
  val groupNodes: HeapTrackingArrayList[ListValue]
  val groupRelationships: HeapTrackingArrayList[ListValue]
  val iterations: Int
  def close(): Unit
}

case class SlottedTrailState(
  constraint: TrailModeConstraint,
  node: Long,
  groupNodes: HeapTrackingArrayList[ListValue],
  groupRelationships: HeapTrackingArrayList[ListValue],
  relationshipsSeen: HeapTrackingLongHashSet,
  iterations: Int,
  closeGroupsOnClose: Boolean
) extends TrailState with SlottedRepeatState with Measurable {
  override def estimatedHeapUsage(): Long = SlottedTrailState.SHALLOW_SIZE

  def close(): Unit = {
    if (closeGroupsOnClose) {
      groupNodes.close()
      groupRelationships.close()
    }
    relationshipsSeen.close()
  }
}

object SlottedTrailState {
  final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[SlottedTrailState])
}

case class SlottedWalkState(
  node: Long,
  groupNodes: HeapTrackingArrayList[ListValue],
  groupRelationships: HeapTrackingArrayList[ListValue],
  iterations: Int,
  closeGroupsOnClose: Boolean
) extends SlottedRepeatState {

  def close(): Unit = {
    if (closeGroupsOnClose) {
      groupNodes.close()
      groupRelationships.close()
    }
  }
}

case class RepeatSlottedPipe(
  source: Pipe,
  inner: Pipe,
  repetition: Repetition,
  startSlot: Slot,
  endOffset: Int,
  innerStarOffset: Int,
  innerEndSlot: Slot,
  groupNodes: Array[GroupSlot],
  groupRelationships: Array[GroupSlot],
  uniquenessConstraint: TraversalModeConstraint,
  slots: SlotConfiguration,
  rhsSlots: SlotConfiguration,
  argumentSize: SlotConfiguration.Size,
  reverseGroupVariableProjections: Boolean
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  private[this] val emptyGroupNodes = emptyLists(groupNodes.length)
  private[this] val emptyGroupRelationships = emptyLists(groupRelationships.length)
  private[this] val getStartNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(startSlot)

  private def createNewState(outerRow: CypherRow, startNode: Long, tracker: MemoryTracker): SlottedRepeatState =
    uniquenessConstraint match {
      case constraint @ RepeatSlottedPipe.TrailModeConstraint(
          _,
          _,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups
        ) =>
        val relationshipsSeen = HeapTrackingCollections.newLongSet(tracker)
        val ir = previouslyBoundRelationships.iterator
        while (ir.hasNext) {
          relationshipsSeen.add(outerRow.getLongAt(ir.next().offset))
        }

        val ig = previouslyBoundRelationshipGroups.iterator
        while (ig.hasNext) {
          val i = castOrFail[ListValue](outerRow.getRefAt(ig.next().offset)).iterator()
          while (i.hasNext) {
            relationshipsSeen.add(castOrFail[VirtualRelationshipValue](i.next()).id())
          }
        }

        SlottedTrailState(
          constraint,
          startNode,
          emptyGroupNodes,
          emptyGroupRelationships,
          relationshipsSeen,
          iterations = 1,
          // empty groups are reused for every argument, so can not be closed until the whole query finishes
          closeGroupsOnClose = false
        )

      case WalkModeConstraint =>
        SlottedWalkState(
          startNode,
          emptyGroupNodes,
          emptyGroupRelationships,
          iterations = 1,
          // empty groups are reused for every argument, so can not be closed until the whole query finishes
          closeGroupsOnClose = false
        )
    }

  private def createNextState(
    state: SlottedRepeatState,
    row: CypherRow,
    innerEndNode: Long,
    tracker: MemoryTracker
  ): SlottedRepeatState =
    state match {
      case trailState: SlottedTrailState =>
        val newSet = HeapTrackingCollections.newLongSet(tracker, trailState.relationshipsSeen)

        var i = 0
        while (i < trailState.constraint.innerRelationships.length) {
          val r = trailState.constraint.innerRelationships(i)
          if (!newSet.add(row.getLongAt(r.offset))) {
            throw new IllegalStateException(
              "Method should only be called when all relationships are known to be unique"
            )
          }
          i += 1
        }

        SlottedTrailState(
          trailState.constraint,
          innerEndNode,
          RepeatSlottedPipe.computeNodeGroupVariables(groupNodes, trailState.groupNodes, row, tracker),
          RepeatSlottedPipe.computeRelGroupVariables(
            groupRelationships,
            trailState.groupRelationships,
            row,
            tracker
          ),
          newSet,
          trailState.iterations + 1,
          closeGroupsOnClose = true
        )

      case walkState: SlottedWalkState =>
        SlottedWalkState(
          innerEndNode,
          RepeatSlottedPipe.computeNodeGroupVariables(groupNodes, walkState.groupNodes, row, tracker),
          RepeatSlottedPipe.computeRelGroupVariables(
            groupRelationships,
            walkState.groupRelationships,
            row,
            tracker
          ),
          walkState.iterations + 1,
          closeGroupsOnClose = true
        )
    }

  private def filterRow(row: CypherRow, repeatState: SlottedRepeatState): Boolean =
    repeatState match {
      case trailState: SlottedTrailState =>
        var relationshipsAreUnique = true
        var i = 0
        val innerRelationshipsSeen = collection.mutable.Set[Long]()
        while (relationshipsAreUnique && i < trailState.constraint.innerRelationships.length) {
          val rel = row.getLongAt(trailState.constraint.innerRelationships(i).offset)
          if (trailState.relationshipsSeen.contains(rel)) {
            relationshipsAreUnique = false
          }
          if (relationshipsAreUnique && !innerRelationshipsSeen.add(rel)) {
            relationshipsAreUnique = false
          }
          i += 1
        }
        relationshipsAreUnique
      case _: SlottedWalkState => true
    }

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    val tracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    input.flatMap { outerRow =>
      def newResultRowWithEmptyGroups(innerEndNode: Long): Some[SlottedRow] = {
        val resultRow = SlottedRow(slots)
        resultRow.copyFrom(outerRow, argumentSize.nLongs, argumentSize.nReferences)
        RepeatSlottedPipe.writeResultColumnsWithProvidedGroups(
          emptyGroupNodes,
          emptyGroupRelationships,
          innerEndNode,
          resultRow,
          groupNodes,
          groupRelationships,
          endOffset
        )
        Some(resultRow)
      }

      def newResultRow(
        rhsInnerRow: CypherRow,
        prevRepetitionGroupNodes: HeapTrackingArrayList[ListValue],
        prevRepetitionGroupRelationships: HeapTrackingArrayList[ListValue],
        innerEndNode: Long
      ): Some[CypherRow] = {
        RepeatSlottedPipe.writeResultColumns(
          prevRepetitionGroupNodes,
          prevRepetitionGroupRelationships,
          innerEndNode,
          rhsInnerRow,
          groupNodes,
          groupRelationships,
          endOffset,
          reverseGroupVariableProjections
        )
        Some(rhsInnerRow)
      }

      val startNode = getStartNodeFunction.applyAsLong(outerRow)
      if (NullChecker.entityIsNull(startNode)) {
        ClosingIterator.empty
      } else {
        // Every invocation of RHS for this incoming LHS row can use the same argument row (avoids unnecessary row creation)
        val rhsInitialRow = SlottedRow(rhsSlots)
        rhsInitialRow.copyFrom(outerRow, argumentSize.nLongs, argumentSize.nReferences)

        val stack = newArrayDeque[SlottedRepeatState](tracker)
        if (repetition.max.isGreaterThan(0)) {
          stack.push(createNewState(outerRow, startNode, tracker))
        }
        new PrefetchingIterator[CypherRow] {
          private var innerResult: ClosingIterator[CypherRow] = ClosingIterator.empty
          private var stackHead: SlottedRepeatState = _
          private var emitFirst = repetition.min == 0

          override protected[this] def closeMore(): Unit = {
            if (stackHead != null) {
              stackHead.close()
            }
            innerResult.close()
            stack.close()
          }

          @tailrec
          def produceNext(): Option[CypherRow] = {
            if (emitFirst) {
              emitFirst = false
              newResultRowWithEmptyGroups(startNode)
            } else if (innerResult.hasNext) {
              val row = innerResult.next()
              val innerEndNode = row.getLongAt(innerEndSlot.offset)
              if (repetition.max.isGreaterThan(stackHead.iterations)) {
                stack.push(createNextState(stackHead, row, innerEndNode, tracker))
              }
              // if iterated long enough emit, otherwise recurse
              if (stackHead.iterations >= repetition.min) {
                newResultRow(row, stackHead.groupNodes, stackHead.groupRelationships, innerEndNode)
              } else {
                produceNext()
              }
            } else if (!stack.isEmpty) {
              // close previous state
              if (stackHead != null) {
                stackHead.close()
              }
              // Run RHS with previous end-node as new innerStartNode
              stackHead = stack.pop()
              rhsInitialRow.setLongAt(innerStarOffset, stackHead.node)

              stackHead match {
                case t: SlottedTrailState =>
                  rhsInitialRow.setRefAt(t.constraint.trailStateMetadataSlot, RuntimeMetadataValue(t))
                case _: SlottedWalkState => ()
              }

              val innerState = state.withInitialContext(rhsInitialRow)
              innerResult = inner.createResults(innerState).filter(filterRow(_, stackHead))
              produceNext()
            } else {
              if (stackHead != null) {
                stackHead.close()
                stackHead = null
              }
              None
            }
          }
        }
      }
    }
  }
}

object RepeatSlottedPipe {

  sealed trait TraversalModeConstraint

  case class TrailModeConstraint(
    trailStateMetadataSlot: Int,
    innerRelationships: Array[Slot],
    previouslyBoundRelationships: Array[Slot],
    previouslyBoundRelationshipGroups: Array[Slot]
  ) extends TraversalModeConstraint

  case object WalkModeConstraint extends TraversalModeConstraint

  def computeNodeGroupVariables(
    groupNodeSlots: Array[GroupSlot],
    groupVariables: HeapTrackingArrayList[ListValue],
    row: ReadableRow,
    tracker: MemoryTracker
  ): HeapTrackingArrayList[ListValue] = {
    val res = HeapTrackingCollections.newArrayList[ListValue](groupVariables.size(), tracker)
    var i = 0
    while (i < groupNodeSlots.length) {
      res.add(groupVariables.get(i).append(VirtualValues.node(row.getLongAt(groupNodeSlots(i).innerSlot.offset))))
      i += 1
    }
    res
  }

  def computeRelGroupVariables(
    groupRelSlots: Array[GroupSlot],
    groupVariables: HeapTrackingArrayList[ListValue],
    row: ReadableRow,
    tracker: MemoryTracker
  ): HeapTrackingArrayList[ListValue] = {
    val res = HeapTrackingCollections.newArrayList[ListValue](groupVariables.size(), tracker)
    var i = 0
    while (i < groupRelSlots.length) {
      res.add(groupVariables.get(i).append(
        VirtualValues.relationship(row.getLongAt(groupRelSlots(i).innerSlot.offset))
      ))
      i += 1
    }
    res
  }

  def writeResultColumns(
    groupNodes: HeapTrackingArrayList[ListValue],
    groupRels: HeapTrackingArrayList[ListValue],
    innerEndNode: Long,
    row: CypherRow,
    groupNodeSlots: Array[GroupSlot],
    groupRelSlots: Array[GroupSlot],
    endOffset: Int,
    reverseGroupVariableProjections: Boolean
  ): Unit = {
    var i = 0
    while (i < groupNodeSlots.length) {
      val nodeGroup = groupNodes.get(i).append(VirtualValues.node(row.getLongAt(groupNodeSlots(i).innerSlot.offset)))
      val projectedNodeGroup = if (reverseGroupVariableProjections) nodeGroup.reverse() else nodeGroup
      row.setRefAt(groupNodeSlots(i).outerSlot.offset, projectedNodeGroup)
      i += 1
    }

    i = 0
    while (i < groupRelSlots.length) {
      val relGroup = groupRels.get(i).append(
        VirtualValues.relationship(row.getLongAt(groupRelSlots(i).innerSlot.offset))
      )
      val projectedRelGroup = if (reverseGroupVariableProjections) relGroup.reverse() else relGroup
      row.setRefAt(groupRelSlots(i).outerSlot.offset, projectedRelGroup)
      i += 1
    }

    row.setLongAt(endOffset, innerEndNode)
  }

  def writeResultColumnsWithProvidedGroups(
    groupNodes: HeapTrackingArrayList[ListValue],
    groupRels: HeapTrackingArrayList[ListValue],
    innerEndNode: Long,
    resultRow: WritableRow,
    groupNodeSlots: Array[GroupSlot],
    groupRelSlots: Array[GroupSlot],
    endOffset: Int
  ): Unit = {
    var i = 0
    while (i < groupNodeSlots.length) {
      resultRow.setRefAt(groupNodeSlots(i).outerSlot.offset, groupNodes.get(i))
      i += 1
    }
    i = 0
    while (i < groupRelSlots.length) {
      resultRow.setRefAt(groupRelSlots(i).outerSlot.offset, groupRels.get(i))
      i += 1
    }
    resultRow.setLongAt(endOffset, innerEndNode)
  }
}

case class GroupSlot(innerSlot: Slot, outerSlot: Slot)
