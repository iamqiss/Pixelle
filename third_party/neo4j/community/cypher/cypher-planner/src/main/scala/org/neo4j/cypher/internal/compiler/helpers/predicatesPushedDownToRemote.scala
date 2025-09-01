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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.Variable

/**
 * This is a utility method to identify if a predicate may be pushed down to the shards while fetching properties.
 */
object predicatesPushedDownToRemote {

  def apply(expression: Expression): Boolean =
    expression.dependencies.size == 1 &&
      containsPropertyAccess(expression) &&
      isSupported(expression)

  private def containsPropertyAccess(expression: Expression): Boolean =
    expression.folder.treeExists {
      case _: Property       => true
      case _: CachedProperty => true
    }

  private def isSupported(expression: Expression): Boolean = expression match {
    case Contains(lhs, rhs)                            => isSupported(lhs) && isSupported(rhs)
    case EndsWith(lhs, rhs)                            => isSupported(lhs) && isSupported(rhs)
    case StartsWith(lhs, rhs)                          => isSupported(lhs) && isSupported(rhs)
    case LessThan(lhs, rhs)                            => isSupported(lhs) && isSupported(rhs)
    case LessThanOrEqual(lhs, rhs)                     => isSupported(lhs) && isSupported(rhs)
    case GreaterThanOrEqual(lhs, rhs)                  => isSupported(lhs) && isSupported(rhs)
    case GreaterThan(lhs, rhs)                         => isSupported(lhs) && isSupported(rhs)
    case Equals(lhs, rhs)                              => isSupported(lhs) && isSupported(rhs)
    case In(lhs, rhs)                                  => isSupported(lhs) && isSupported(rhs)
    case NotEquals(lhs, rhs)                           => isSupported(lhs) && isSupported(rhs)
    case IsNull(expr)                                  => isSupported(expr)
    case IsNotNull(expr)                               => isSupported(expr)
    case Property(expr, _)                             => isSupported(expr)
    case ListLiteral(expressions)                      => expressions.forall(isSupported)
    case AndedPropertyInequalities(_, _, inequalities) => inequalities.forall(isSupported)
    case _: CachedProperty                             => true
    case _: Parameter                                  => true
    case _: Variable                                   => true
    case _: Literal                                    => true
    case _                                             => false
  }
}
