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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.Value

object PipeHelper {

  /**
   * Evaluate a statically known expressions. Assert that it is a long value.
   *
   * @param exp      the expression
   * @param minValue validation number to check if `exp` is a valid long. (Larger than min)
   * @param state    the query state
   * @param prefix   a prefix for error messages
   * @param suffix   a suffix for error messages
   * @return the number
   */
  def evaluateStaticLongOrThrow(
    exp: Expression,
    minValue: Long,
    state: QueryState,
    prefix: String,
    suffix: String
  ): Long = {
    def fail(n: Any): Unit = {
      val nValue = n match {
        case value: Value => value.prettyPrint()
        case other        => String.valueOf(other)
      }
      if (n.isInstanceOf[FloatingPointValue]) {
        throw InvalidArgumentException.invalidDoubleValuePrefixSuffix(String.valueOf(n), prefix, suffix, nValue)
      } else
        throw InvalidArgumentException.invalidValuePrefixSuffix(minValue, String.valueOf(n), prefix, suffix, nValue)
    }

    val number = NumericHelper.evaluateStaticallyKnownNumber(exp, state)
    if (number.isInstanceOf[FloatingPointValue]) {
      val n = number.doubleValue()
      fail(n)
    }
    val res = number.longValue()
    if (res < minValue) {
      fail(res)
    }
    res
  }
}
