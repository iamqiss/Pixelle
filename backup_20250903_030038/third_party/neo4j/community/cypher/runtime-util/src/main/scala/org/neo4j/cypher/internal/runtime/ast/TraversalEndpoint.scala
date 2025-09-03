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
package org.neo4j.cypher.internal.runtime.ast

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.Endpoint
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint.STRINGIFIER

/**
 * This expression resolves to a node reference during certain relationship traversals
 * (VarExpand and Shortest, for example). The End field refers to which node of the relationship should be returned,
 * relative to the direction of traversal (note: not related to the direction of the relationship itself).
 */
case class TraversalEndpoint(tempVar: LogicalVariable, endpoint: Endpoint) extends RuntimeExpression {
  def isConstantForQuery: Boolean = false
  override def asCanonicalStringVal: String = STRINGIFIER(this)
}

object TraversalEndpoint {
  sealed trait Endpoint

  object Endpoint {
    case object From extends Endpoint
    case object To extends Endpoint
  }

  def extract(expr: Expression): Seq[AllocatedTraversalEndpoint] =
    expr.folder.treeCollect {
      case TraversalEndpoint(v, endpoint) => AllocatedTraversalEndpoint(ExpressionVariable.cast(v).offset, endpoint)
    }

  case class AllocatedTraversalEndpoint(exprVarOffset: Int, endpoint: Endpoint)

  val STRINGIFIER: ExpressionStringifier = ExpressionStringifier(
    extensionStringifier = TraversalEndpointExpressionStringify,
    alwaysParens = false,
    alwaysBacktick = false,
    preferSingleQuotes = true,
    sensitiveParamsAsParams = false
  )

  private object TraversalEndpointExpressionStringify extends ExpressionStringifier.Extension {

    override def apply(ctx: ExpressionStringifier)(expression: Expression): String = expression match {
      case TraversalEndpoint(_, direction) => direction match {
          case TraversalEndpoint.Endpoint.From => "FROM"
          case TraversalEndpoint.Endpoint.To   => "TO"
          case _                               => ""
        }
      case e => e.asCanonicalStringVal
    }
  }
}
