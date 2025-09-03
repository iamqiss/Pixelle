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
package org.neo4j.cypher.internal.optionsmap

import org.neo4j.cypher.internal.util.InternalNotification

sealed trait OptionsConverterResult[+T] {
  def get: T
  def toOption: Option[T]
  def toOptionNotification: (Option[T], Set[InternalNotification])
}

case object Nothing extends OptionsConverterResult[Nothing] {
  override def get: Nothing = throw new NoSuchElementException("Nothing.get")
  override def toOption: Option[Nothing] = None
  override def toOptionNotification: (Option[Nothing], Set[InternalNotification]) = (toOption, Set.empty)
}

case class ParsedOptions[T](result: T) extends OptionsConverterResult[T] {
  override def get: T = result
  override def toOption: Option[T] = Some(result)
  override def toOptionNotification: (Option[T], Set[InternalNotification]) = (toOption, Set.empty)
}

case class ParsedWithNotifications[T](result: T, notifications: Set[InternalNotification])
    extends OptionsConverterResult[T] {
  override def get: T = result
  override def toOption: Option[T] = Some(result)
  override def toOptionNotification: (Option[T], Set[InternalNotification]) = (toOption, notifications)
}
