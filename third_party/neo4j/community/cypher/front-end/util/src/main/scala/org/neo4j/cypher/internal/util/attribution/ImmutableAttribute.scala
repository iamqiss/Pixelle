/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.attribution

import org.eclipse.collections.api.map.primitive.ImmutableIntObjectMap
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag

trait ImmutableAttribute[T] {
  def isDefinedAt(id: Id): Boolean
  def apply(id: Id): T
  def orderedIterator: Iterator[(Id, T)]
  final def get(id: Id): T = apply(id)
  final def getOrElse(id: Id, other: => T): T = if (isDefinedAt(id)) apply(id) else other

  final override def equals(obj: Any): Boolean = obj match {
    case other: ImmutableAttribute[_] => orderedIterator sameElements other.orderedIterator
    case _                            => false
  }

  final override def hashCode(): Int = {
    val iter = orderedIterator
    var res = 0
    while (iter.hasNext) res = 31 * res + iter.next().hashCode()
    res
  }

  final override def toString: String = orderedIterator.mkString("ImmutableAttribute(", ", ", ")")
}

object ImmutableAttribute {
  final private val ARRAY_SIZE_THRESHOLD: Int = 64

  def empty[T: ClassTag]: ImmutableAttribute[T] = new ArraySeqImmutableAttribute(ArraySeq.empty[T])

  /** Returns a new ImmutableAttribute of the specified values. */
  def apply[T: ClassTag](values: Seq[(Id, T)]): ImmutableAttribute[T] = {
    val arraySize = values.foldLeft(0) { case (acc, (id, _)) => math.max(acc, id.x + 1) }

    if (arraySize > ARRAY_SIZE_THRESHOLD && arraySize > values.size * 3) {
      // Lots of unpopulated ids, use map based implementation to optimise for space.
      val map = IntObjectMaps.mutable.ofInitialCapacity[T](values.size)
      values.foreach { case (id, value) => map.put(id.x, value) }
      new MapImmutableAttribute(map.toImmutable)
    } else {
      // Few unpopulated ids, use array based implementation.
      val array = new Array[T](arraySize)
      values.foreach { case (id, v) => array(id.x) = v }
      new ArraySeqImmutableAttribute(ArraySeq.unsafeWrapArray(array))
    }
  }

  /** Returns a new ImmutableAttribute with de-duplicated values. */
  def deduplicated[T: ClassTag](values: Iterator[(Id, T)]): ImmutableAttribute[T] = {
    apply(
      values.toSeq
        .groupMap { case (_, value) => value } { case (id, _) => id }
        .iterator
        .flatMap { case (value, ids) => ids.map(id => id -> value) }
        .toSeq
    )
  }

  final private class ArraySeqImmutableAttribute[T](
    values: ArraySeq[T]
  ) extends ImmutableAttribute[T] {
    override def isDefinedAt(id: Id): Boolean = id.x < values.size && values(id.x) != null

    override def apply(id: Id): T =
      if (isDefinedAt(id)) values(id.x)
      else throw new NoSuchElementException("Key not found " + id)

    override def orderedIterator: Iterator[(Id, T)] = Range(0, values.size).iterator
      .collect { case i if isDefinedAt(Id(i)) => Id(i) -> values(i) }
  }

  final private class MapImmutableAttribute[T](
    values: ImmutableIntObjectMap[T]
  ) extends ImmutableAttribute[T] {
    private val maxId: Int = values.keysView().maxIfEmpty(-1)
    override def isDefinedAt(id: Id): Boolean = values.containsKey(id.x)

    override def apply(id: Id): T =
      if (isDefinedAt(id)) values.get(id.x)
      else throw new NoSuchElementException("Key not found " + id)

    override def orderedIterator: Iterator[(Id, T)] = Range.inclusive(0, maxId).iterator.collect {
      case id if isDefinedAt(Id(id)) => Id(id) -> values.get(id)
    }
  }
}
