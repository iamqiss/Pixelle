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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.RightUnaryOperatorExpression
import org.neo4j.cypher.internal.expressions.TypeSignature
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CypherType

case class IsTyped(lhs: Expression, typeName: CypherType)(
  val position: InputPosition,
  val withDoubleColonOnly: Boolean
) extends BooleanExpression
    with RightUnaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  def copyAndKeepMeta(lhs: Expression = lhs, typeName: CypherType = typeName): IsTyped =
    copy(lhs = lhs, typeName = typeName)(position, withDoubleColonOnly)

  override def canonicalOperatorSymbol = "IS ::"

  override def dup(children: Seq[AnyRef]): this.type =
    children.size match {
      case 2 =>
        IsTyped(
          children.head.asInstanceOf[Expression],
          children(1).asInstanceOf[CypherType]
        )(position, withDoubleColonOnly).asInstanceOf[this.type]
      case 3 =>
        IsTyped(
          children.head.asInstanceOf[Expression],
          children(1).asInstanceOf[CypherType]
        )(
          children(2).asInstanceOf[InputPosition],
          withDoubleColonOnly
        ).asInstanceOf[this.type]
      case 4 =>
        IsTyped(
          children.head.asInstanceOf[Expression],
          children(1).asInstanceOf[CypherType]
        )(
          children(2).asInstanceOf[InputPosition],
          children(3).asInstanceOf[Boolean]
        ).asInstanceOf[this.type]
      case _ => throw new IllegalStateException("IsTyped has at least 2 and at most 4 children.")
    }
}

object IsTyped {

  val withDoubleColonOnlyDefault: Boolean = false
}

case class IsNotTyped(lhs: Expression, typeName: CypherType)(val position: InputPosition)
    extends BooleanExpression
    with RightUnaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )

  def copyAndKeepMeta(lhs: Expression = lhs, typeName: CypherType = typeName): IsNotTyped =
    copy(lhs = lhs, typeName = typeName)(position)

  override def canonicalOperatorSymbol = "IS NOT ::"
}
