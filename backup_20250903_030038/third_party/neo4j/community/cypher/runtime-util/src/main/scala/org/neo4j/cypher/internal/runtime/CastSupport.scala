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

import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.operations.CypherTypeValueMapper
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

import scala.reflect.ClassTag

object CastSupport {

  private val CYPHER_TYPE_NAME_VALUE_MAPPER = new CypherTypeValueMapper

  def valueType(in: AnyValue): String = CypherType.normalizeTypes(in.map(CYPHER_TYPE_NAME_VALUE_MAPPER)).description

  def castOrFail[A <: AnyValue](value: AnyValue)(implicit ev: ClassTag[A]): A = value match {
    case v: A => v
    case _    => throw typeError[A](value)
  }

  def typeError[A <: AnyValue](value: AnyValue)(implicit ev: ClassTag[A]): CypherTypeException = {
    value match {
      case value: Value => throw CypherTypeException.expectedOtherType(
          String.valueOf(value),
          ev.runtimeClass.getName,
          value.getClass.getName,
          value.prettyPrint(),
          CypherTypeValueMapper.valueType(value)
        )
      case other => throw CypherTypeException.expectedOtherType(
          String.valueOf(other),
          ev.runtimeClass.getName,
          other.getClass.getName,
          String.valueOf(other),
          CypherTypeValueMapper.valueType(other)
        )
    }
  }
}
