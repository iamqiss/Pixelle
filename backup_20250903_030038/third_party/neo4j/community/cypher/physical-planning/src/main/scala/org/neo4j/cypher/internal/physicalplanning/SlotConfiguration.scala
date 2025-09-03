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
package org.neo4j.cypher.internal.physicalplanning

import org.eclipse.collections.api.map.ImmutableMap
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.DuplicatedSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.KeyedSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.MetaDataSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.OuterNestedApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.keyIs
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.missingArg
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.missingNestedArg
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.exceptions.InternalException
import org.neo4j.util.VisibleForTesting

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag

/**
 * Immutable slot configuration.
 * Contains array offsets and type information of the driving table columns.
 */
final case class SlotConfiguration private (
  slots: ArraySeq[KeyedSlot], // Ordered by key type, long/ref slot, offset
  numberOfLongs: Int,
  numberOfReferences: Int,
  discardedRefSlotOffsets: ArraySeq[Int]
) extends SlotView[KeyedSlot] {

  private[this] val slotsByKey: ImmutableMap[SlotKey, KeyedSlot] = {
    val map = new UnifiedMap[SlotKey, KeyedSlot](slots.length)
    slots.foreach { s =>
      map.put(s.key, s)
      s.aliases.foreach(alias => map.put(VariableSlotKey(alias), s))
    }
    map.toImmutable
  }

  /** Ref slot offsets of all cached properties. */
  val cachedPropertyOffsets: ArraySeq[Int] = slots.view
    .collect { case s if keyIs[CachedPropertySlotKey](s) => s.offset }
    .to(ArraySeq)

  /** Index of `slots` with the first cached property offset, or -1. */
  val cachedPropertiesStartIndex: Int = Range(0, slots.size)
    .collectFirst { case i if keyIs[CachedPropertySlotKey](slots(i)) => i }
    .getOrElse(-1)

  /** Index of `slots` with the last (exclusive) cached property offset, or -1. */
  val cachedPropertiesEndIndex: Int = Range(0, slots.size).reverse
    .collectFirst { case i if keyIs[CachedPropertySlotKey](slots(i)) => i + 1 }
    .getOrElse(-1)

  override def apply(key: SlotKey): KeyedSlot = {
    val value = slotsByKey.get(key)
    if (value eq null) throw new NoSuchElementException(s"Failed to find slot for $key")
    value
  }

  def getOrElse(key: SlotKey, f: => KeyedSlot): KeyedSlot = {
    val value = slotsByKey.get(key)
    if (value ne null) value
    else f
  }
  override def get(key: SlotKey): Option[KeyedSlot] = Option(slotsByKey.get(key))
  override def getSlot(key: SlotKey): Option[Slot] = get(key).map(_.slot)
  override def contains(key: SlotKey): Boolean = slotsByKey.containsKey(key)

  override def longOffset(key: SlotKey): Int = {
    val slot = apply(key)
    require(slot.isLongSlot)
    slot.offset
  }

  override def refOffset(key: SlotKey): Int = {
    val slot = apply(key)
    require(!slot.isLongSlot)
    slot.offset
  }

  def size(): SlotConfiguration.Size = SlotConfiguration.Size(numberOfLongs, numberOfReferences)

  def aliasesOf(variable: String): Seq[String] = get(VariableSlotKey(variable)) match {
    case Some(slot) if slot.key == VariableSlotKey(variable) => slot.aliases
    case _                                                   => Seq.empty
  }

  def nameOfSlot(offset: Int, longSlot: Boolean): Option[String] = slots.collectFirst {
    case KeyedSlot(VariableSlotKey(name), s, aliases)
      if s.isLongSlot == longSlot && s.offset == offset && !aliases.contains(name) => name
  }

  def legacyView: LegacySlotView = new LegacySlotView {

    override def iterable: Iterable[(SlotKey, Slot)] = SlotConfiguration.this.slots.view
      .map(s => s.key -> s.slot)

    override def iterableWithAliases: Iterable[(SlotKey, Slot)] = SlotConfiguration.this.slots.view
      .flatMap { s =>
        val slot = s.slot
        Seq(s.key -> slot).concat(s.aliases.view.map(alias => VariableSlotKey(alias) -> slot))
      }

    override def keyedSlots: Iterable[SlotWithKeyAndAliases] = SlotConfiguration.this.slots.view
      .map(s => SlotWithKeyAndAliases(s.key, s.slot, s.aliases.toSet))
  }

  def hasCachedProperties: Boolean = cachedPropertyOffsets.nonEmpty
}

object SlotConfiguration {
  val empty: SlotConfiguration = SlotConfigurationBuilder.empty.build()

  def apply(mutable: SlotConfigurationBuilder): SlotConfiguration = {
    val slots = mutable.keyedSlots.map(s => KeyedSlot(s.key, s.slot, s.aliases.toIndexedSeq)).toSeq
    new SlotConfiguration(
      slots = slots.sortBy(s => (keyOrdering(s.key), !s.slot.isLongSlot, s.offset)).to(ArraySeq),
      numberOfLongs = mutable.numberOfLongs,
      numberOfReferences = mutable.numberOfReferences,
      discardedRefSlotOffsets = ArraySeq.unsafeWrapArray(mutable.discardedRefOffsets())
    )
  }

  @VisibleForTesting
  def apply(input: Seq[SlotWithKeyAndAliases], longs: Int, refs: Int): SlotConfiguration = {
    val slots = input.map(s => KeyedSlot(s.key, s.slot, s.aliases.toIndexedSeq))
    new SlotConfiguration(
      slots = slots.sortBy(s => (keyOrdering(s.key), !s.slot.isLongSlot, s.offset)).to(ArraySeq),
      numberOfLongs = longs,
      numberOfReferences = refs,
      discardedRefSlotOffsets = ArraySeq.empty
    )
  }

  private def keyOrdering(key: SlotKey): Int = key match {
    case _: VariableSlotKey             => 0
    case _: CachedPropertySlotKey       => 1
    case _: MetaDataSlotKey             => 2
    case _: DuplicatedSlotKey           => 3
    case _: ApplyPlanSlotKey            => 4
    case _: OuterNestedApplyPlanSlotKey => 5
  }

  def keyIs[K <: SlotKey](s: KeyedSlot)(implicit tag: ClassTag[K]): Boolean = tag.runtimeClass.isInstance(s.key)

  case class Size(nLongs: Int, nReferences: Int) {

    def contains(s: Slot): Boolean = {
      (s.isLongSlot && s.offset < nLongs) || (!s.isLongSlot && s.offset < nReferences)
    }
  }

  object Size {
    val zero: Size = Size(nLongs = 0, nReferences = 0)
  }

  final def isRefSlotAndNotAlias(slots: SlotConfiguration, variable: String): Boolean = {
    val key = VariableSlotKey(variable)
    slots.get(key).forall(s => !s.isLongSlot && s.key == key)
  }

  sealed trait SlotKey

  /** Slot keys needed during execution of queries (included in query caches).  */
  sealed trait RuntimeSlotKey extends SlotKey

  /** Slot keys that are only needed during physical planning of queries (not included in query caches). */
  sealed trait PlanningSlotKey extends SlotKey

  case class VariableSlotKey(name: String) extends RuntimeSlotKey {
    override def hashCode(): Int = name.hashCode // Micro optimised hash code
  }
  case class CachedPropertySlotKey(property: ASTCachedProperty.RuntimeKey) extends RuntimeSlotKey
  case class MetaDataSlotKey(name: String, planId: Id) extends RuntimeSlotKey
  case class DuplicatedSlotKey(name: String, slotId: Int) extends RuntimeSlotKey
  case class ApplyPlanSlotKey(applyPlanId: Id) extends PlanningSlotKey
  case class OuterNestedApplyPlanSlotKey(applyPlanId: Id) extends PlanningSlotKey

  case class SlotWithKeyAndAliases(key: SlotKey, slot: Slot, aliases: collection.Set[String])

  case class KeyedSlot(
    key: SlotKey,
    slot: Slot,
    aliases: IndexedSeq[String]
  ) {
    val slotType: Int = SlotTypeFactory(slot)
    def offset: Int = slot.offset
    def isLongSlot: Boolean = SlotType.isLongSlot(slotType)
    def nullable: Boolean = SlotType.isNullable(slotType)
  }

  object SlotTypeFactory {

    def apply(slot: Slot): Int = slot match {
      case LongSlot(_, false, CTNode)         => SlotType.NodeNonNullLongSlot
      case LongSlot(_, true, CTNode)          => SlotType.NodeNullableLongSlot
      case LongSlot(_, false, CTRelationship) => SlotType.RelNonNullLongSlot
      case LongSlot(_, true, CTRelationship)  => SlotType.RelNullableLongSlot
      case LongSlot(_, false, _)              => SlotType.OtherNonNullLongSlot
      case LongSlot(_, true, _)               => SlotType.OtherNullableLongSlot
      case RefSlot(_, false, CTNode)          => SlotType.NodeNonNullRefSlot
      case RefSlot(_, true, CTNode)           => SlotType.NodeNullableRefSlot
      case RefSlot(_, false, CTRelationship)  => SlotType.RelNonNullRefSlot
      case RefSlot(_, true, CTRelationship)   => SlotType.RelNullableRefSlot
      case RefSlot(_, false, _)               => SlotType.OtherNonNullRefSlot
      case RefSlot(_, true, _)                => SlotType.OtherNullableRefSlot
    }
  }

  def missingArg(id: Id): InternalException =
    new InternalException(s"No argument slot allocated for plan with $id")

  def missingNestedArg(id: Id): InternalException =
    new InternalException(s"No nested argument slot allocated for plan with $id")
}

trait SlotView[S] {
  def apply(key: SlotKey): S
  def get(key: SlotKey): Option[S]
  def getSlot(key: SlotKey): Option[Slot]
  def contains(key: SlotKey): Boolean
  def longOffset(key: SlotKey): Int
  def refOffset(key: SlotKey): Int

  final def apply(key: LogicalVariable): S = apply(VariableSlotKey(key.name))
  final def apply(key: String): S = apply(VariableSlotKey(key))
  final def get(key: LogicalVariable): Option[S] = get(VariableSlotKey(key.name))
  final def get(key: String): Option[S] = get(VariableSlotKey(key))
  final def getSlot(variable: String): Option[Slot] = getSlot(VariableSlotKey(variable))
  final def getSlot(variable: LogicalVariable): Option[Slot] = getSlot(VariableSlotKey(variable.name))
  final def contains(variable: String): Boolean = contains(VariableSlotKey(variable))
  final def longOffset(variable: String): Int = longOffset(VariableSlotKey(variable))
  final def longOffset(variable: LogicalVariable): Int = longOffset(variable.name)
  final def refOffset(variable: String): Int = refOffset(VariableSlotKey(variable))
  final def refOffset(variable: LogicalVariable): Int = refOffset(variable.name)
  final def cachedPropOffset(key: ASTCachedProperty.RuntimeKey): Int = refOffset(CachedPropertySlotKey(key))
  final def metaDataOffset(key: MetaDataSlotKey): Int = refOffset(key)
  final def metaDataOffset(key: String): Int = refOffset(MetaDataSlotKey(key, Id.INVALID_ID))
  final def metaDataOffset(key: String, planId: Id): Int = refOffset(MetaDataSlotKey(key, planId))
  final def refSlot(key: SlotKey): RefSlot = getSlot(key).get.asInstanceOf[RefSlot]
  final def longSlot(key: SlotKey): LongSlot = getSlot(key).get.asInstanceOf[LongSlot]
  final def getRefSlot(key: SlotKey): Option[RefSlot] = getSlot(key).map(_.asInstanceOf[RefSlot])
  final def getLongSlot(key: SlotKey): Option[LongSlot] = getSlot(key).map(_.asInstanceOf[LongSlot])
  final def hasCachedPropertySlot(key: ASTCachedProperty.RuntimeKey): Boolean = contains(CachedPropertySlotKey(key))
  final def hasDuplicateSlot(key: String, offset: Int): Boolean = contains(DuplicatedSlotKey(key, offset))
  final def cachedPropSlot(key: ASTCachedProperty.RuntimeKey): Option[RefSlot] = getRefSlot(CachedPropertySlotKey(key))
  final def hasArgumentSlot(planId: Id): Boolean = contains(ApplyPlanSlotKey(planId))
  final def hasNestedArgumentSlot(planId: Id): Boolean = contains(OuterNestedApplyPlanSlotKey(planId))
  final def getArgumentSlot(planId: Id): Option[LongSlot] = getLongSlot(ApplyPlanSlotKey(planId))
  final def nestedArgumentSlot(planId: Id): Option[LongSlot] = getLongSlot(OuterNestedApplyPlanSlotKey(planId))
  final def hasMetaDataSlot(key: String, id: Id): Boolean = contains(MetaDataSlotKey(key, id))
  final def getMetaDataSlot(key: String, id: Id = Id.INVALID_ID): Option[RefSlot] = getRefSlot(MetaDataSlotKey(key, id))
  final def getMetaDataSlot(key: MetaDataSlotKey): Option[RefSlot] = getRefSlot(key)

  /** Returns the long offset of the specified apply plan id. */
  final def argumentOffset(applyPlanId: Id): Int = applyPlanId match {
    case Id.INVALID_ID => TopLevelArgument.SLOT_OFFSET
    case id            => getSlot(ApplyPlanSlotKey(id)).getOrElse(throw missingArg(id)).offset
  }

  /**
   * Returns the outer-level argument, for an [[ApplyPlan]] that has two-level/nested arguments.
   * E.g., the LHS argument for [[Trail]].
   */
  final def nestedArgumentOffset(applyPlanId: Id): Int = applyPlanId match {
    case Id.INVALID_ID => TopLevelArgument.SLOT_OFFSET
    case id            => getSlot(OuterNestedApplyPlanSlotKey(id)).getOrElse(throw missingNestedArg(id)).offset
  }
}
