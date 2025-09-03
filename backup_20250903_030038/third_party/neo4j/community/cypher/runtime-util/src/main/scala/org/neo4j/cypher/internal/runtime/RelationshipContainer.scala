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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.logical.plans.TraversalMatchMode
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST

trait RelationshipContainer extends AutoCloseable {
  def append(rel: VirtualRelationshipValue): RelationshipContainer
  def canAdd(rel: VirtualRelationshipValue): Boolean
  def canAdd(relId: Long): Boolean
  def reverse: RelationshipContainer
  def size: Int
  def asList: ListValue
}

object RelationshipContainer {

  /**
   * Utility class that has constant time `append`, `contains`, and `size` methods
   */
  private class TrailModeRelationshipContainer private[RelationshipContainer] (
    val asList: ListValue,
    val size: Int,
    set: HeapTrackingLongImmutableSet
  ) extends RelationshipContainer {

    override def append(rel: VirtualRelationshipValue): RelationshipContainer = {
      new TrailModeRelationshipContainer(asList.append(rel), size + 1, set + rel.id())
    }
    override def canAdd(rel: VirtualRelationshipValue): Boolean = !set.contains(rel.id())
    override def canAdd(relId: Long): Boolean = !set.contains(relId)

    override def reverse: RelationshipContainer = new TrailModeRelationshipContainer(asList.reverse(), size, set)

    override def close(): Unit = {
      set.close()
    }
  }

  private class WalkModeRelationshipContainer private[RelationshipContainer] (val asList: ListValue, val size: Int)
      extends RelationshipContainer {

    override def append(rel: VirtualRelationshipValue): RelationshipContainer = {
      new WalkModeRelationshipContainer(asList.append(rel), size + 1)
    }
    override def canAdd(rel: VirtualRelationshipValue): Boolean = true
    override def canAdd(relId: Long): Boolean = true

    override def reverse: RelationshipContainer = new WalkModeRelationshipContainer(asList.reverse(), size)

    override def close(): Unit = {
      // nothing to close
    }
  }

  def empty(memoryTracker: MemoryTracker, traversalMatchMode: TraversalMatchMode): RelationshipContainer =
    traversalMatchMode match {
      case TraversalMatchMode.Walk =>
        new WalkModeRelationshipContainer(EMPTY_LIST, 0)
      case TraversalMatchMode.Trail =>
        new TrailModeRelationshipContainer(EMPTY_LIST, 0, HeapTrackingLongImmutableSet.emptySet(memoryTracker))
    }
}
