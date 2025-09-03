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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.values.AnyValue

abstract class FunctionInvocation(signature: UserFunctionSignature, input: Array[Expression])
    extends Expression with GraphElementPropertyFunctions {

  override def arguments: Seq[Expression] = input

  override def children: Seq[AstNode[_]] = input

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val length = arguments.length
    val argValues = new Array[AnyValue](length)
    var i = 0
    while (i < length) {
      argValues(i) = arguments(i).apply(row, state)
      i += 1
    }

    call(state, argValues)
  }

  protected def call(state: QueryState, argValues: Array[AnyValue]): AnyValue

  override def toString = s"${signature.name}(${input.mkString(",")})"
}

case class BuiltInFunctionInvocation(opId: Id, signature: UserFunctionSignature, input: Array[Expression])
    extends FunctionInvocation(signature, input) {

  override protected def call(state: QueryState, argValues: Array[AnyValue]): AnyValue = {
    val query = state.query
    query.callBuiltInFunction(
      signature.id,
      argValues,
      query.procedureCallContext(signature.id, state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(opId.x))
    )
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(BuiltInFunctionInvocation(opId, signature, input.map(a => a.rewrite(f))))
}

case class UserFunctionInvocation(opId: Id, signature: UserFunctionSignature, input: Array[Expression])
    extends FunctionInvocation(signature, input) {

  override protected def call(state: QueryState, argValues: Array[AnyValue]): AnyValue = {
    val query = state.query
    query.callFunction(
      signature.id,
      argValues,
      query.procedureCallContext(signature.id, state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(opId.x))
    )
  }

  override def rewrite(f: Expression => Expression): Expression =
    f(UserFunctionInvocation(opId, signature, input.map(a => a.rewrite(f))))
}
